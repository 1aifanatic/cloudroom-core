//! Cloud agents schedule work for their own thread (ADR 0212). Core saves each automation straight to the database
//! with its own login, like feedback, so it works while the user's Mac is offline. The website's every-minute cron
//! wakes the sandbox and sends the prompt; it also fills in the next run, so Core never parses cron.
use super::{
    Manager,
    thread::{Failure, caller, fail},
};
use crate::preview::Peer;
use axum::{
    Json, Router,
    extract::{ConnectInfo, State},
    http::StatusCode,
    routing::post,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use std::{io, sync::Arc};

const USAGE: &str = "cloudroom automation create --name NAME --prompt TEXT|--prompt-file PATH (--cron 'MIN HOUR DAY MONTH WEEKDAY' [--timezone ZONE] | --at ISO_TIME | --in 30m)
cloudroom automation list
cloudroom automation update ID [--name NAME] [--prompt TEXT] [--cron EXPR [--timezone ZONE] | --at ISO_TIME | --in 30m]
cloudroom automation pause|resume|run|delete ID
Re-prompts this Cloud thread on a schedule, even while the user's Mac is off. Cloudroom fills in the next run within a minute; check `list` for it or an error.";
const MAX_PROMPT: usize = 32_000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Body {
    action: String,
    id: Option<String>,
    name: Option<String>,
    prompt: Option<String>,
    cron: Option<String>,
    timezone: Option<String>,
    at: Option<String>,
    delay: Option<String>,
}

/// Served on the agent-only socket that `cloudroom mac` also uses.
pub(crate) fn agent_routes() -> Router<Arc<Manager>> {
    Router::new().route("/automation", post(automation))
}

async fn automation(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Json(input): Json<Body>,
) -> Result<Json<Value>, Failure> {
    let session = caller(&m, peer, "automation")?;
    check(&input)?;
    let pool = &m.history.pool;
    let database = |error: sqlx::Error| {
        let message = error.as_database_error().map(|e| e.message().to_owned());
        fail(
            StatusCode::CONFLICT,
            &message.unwrap_or_else(|| format!("Could not save the automation: {error}")),
        )
    };
    let id = || {
        input
            .id
            .as_deref()
            .ok_or(fail(StatusCode::BAD_REQUEST, "Give the automation ID"))
    };
    let changed = |row: Option<sqlx::postgres::PgRow>| match row {
        Some(row) => Ok(Json(json!({"id": row.get::<String, _>(0), "ok": true}))),
        None => Err(fail(StatusCode::NOT_FOUND, "Automation not found")),
    };
    match input.action.as_str() {
        "create" => {
            let Some(sandbox) = m.config.instance.as_deref() else {
                return Err(fail(StatusCode::CONFLICT, "Run cloudroom automation from a Cloudroom cloud thread"));
            };
            let row = sqlx::query("SELECT cloudroom_private.core_add_automation($1, $2, $3, $4, $5, $6, coalesce($7::timestamptz, now() + $8::interval))::text")
                .bind(sandbox).bind(&session).bind(&input.name).bind(&input.prompt).bind(&input.cron)
                .bind(&input.timezone).bind(&input.at).bind(&input.delay)
                .fetch_one(pool).await.map_err(database)?;
            let id: String = row.get(0);
            Ok(Json(json!({"id": id, "note": "Created. Cloudroom fills in the next run within a minute: check `cloudroom automation list`."})))
        }
        "list" => {
            let rows = sqlx::query("SELECT id::text, name, prompt, cron, timezone, run_at::text, enabled, next_run_at::text, last_run_at::text, last_error, runs FROM cloudroom_automations ORDER BY created_at LIMIT 100")
                .fetch_all(pool).await.map_err(database)?;
            let list: Vec<Value> = rows.iter().map(|r| json!({
                "id": r.get::<String, _>(0), "name": r.get::<String, _>(1), "prompt": r.get::<String, _>(2),
                "cron": r.get::<Option<String>, _>(3), "timezone": r.get::<String, _>(4), "run_at": r.get::<Option<String>, _>(5),
                "enabled": r.get::<bool, _>(6), "next_run_at": r.get::<Option<String>, _>(7), "last_run_at": r.get::<Option<String>, _>(8),
                "last_error": r.get::<Option<String>, _>(9), "runs": r.get::<i32, _>(10),
            })).collect();
            Ok(Json(json!({"automations": list})))
        }
        // A new schedule or a resume clears the next run; the website computes it within a minute.
        "update" => changed(sqlx::query("WITH s AS (SELECT coalesce($6::timestamptz, now() + $7::interval) AS at)
            UPDATE cloudroom_automations SET name = coalesce($2, name), prompt = coalesce($3, prompt),
              cron = CASE WHEN $4::text IS NOT NULL THEN $4 WHEN (SELECT at FROM s) IS NOT NULL THEN NULL ELSE cron END,
              run_at = CASE WHEN $4::text IS NOT NULL THEN NULL WHEN (SELECT at FROM s) IS NOT NULL THEN (SELECT at FROM s) ELSE run_at END,
              timezone = coalesce($5, timezone),
              next_run_at = CASE WHEN $4::text IS NOT NULL OR $5::text IS NOT NULL OR (SELECT at FROM s) IS NOT NULL THEN NULL ELSE next_run_at END,
              updated_at = now()
            WHERE id = $1::uuid RETURNING id::text")
            .bind(id()?).bind(&input.name).bind(&input.prompt).bind(&input.cron).bind(&input.timezone).bind(&input.at).bind(&input.delay)
            .fetch_optional(pool).await.map_err(database)?),
        action @ ("pause" | "resume" | "run") => {
            let set = match action {
                "pause" => "enabled = false, next_run_at = NULL",
                "resume" => "enabled = true, next_run_at = NULL",
                _ => "enabled = true, next_run_at = now()",
            };
            changed(sqlx::query(&format!("UPDATE cloudroom_automations SET {set}, updated_at = now() WHERE id = $1::uuid RETURNING id::text"))
                .bind(id()?).fetch_optional(pool).await.map_err(database)?)
        }
        "delete" => changed(sqlx::query("DELETE FROM cloudroom_automations WHERE id = $1::uuid RETURNING id::text")
            .bind(id()?).fetch_optional(pool).await.map_err(database)?),
        _ => Err(fail(StatusCode::BAD_REQUEST, "Unknown automation action")),
    }
}

/// Clear errors before the database: the website checks the cron itself and reports a bad one in `list`.
fn check(input: &Body) -> Result<(), Failure> {
    let bad = |message: &str| Err(fail(StatusCode::BAD_REQUEST, message));
    if let Some(id) = &input.id
        && (id.len() != 36 || !id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-'))
    {
        return bad("Invalid automation ID");
    }
    if input
        .name
        .as_ref()
        .is_some_and(|n| n.trim().is_empty() || n.chars().count() > 200)
    {
        return bad("Use a name of 1-200 characters");
    }
    if input
        .prompt
        .as_ref()
        .is_some_and(|p| p.trim().is_empty() || p.len() > MAX_PROMPT)
    {
        return bad("Use a prompt of 1-32000 bytes");
    }
    if input
        .cron
        .as_ref()
        .is_some_and(|c| c.split_whitespace().count() != 5)
    {
        return bad("Use a 5-field cron: minute hour day month weekday");
    }
    let schedules = [
        input.cron.is_some(),
        input.at.is_some(),
        input.delay.is_some(),
    ]
    .iter()
    .filter(|s| **s)
    .count();
    if schedules > 1
        || (input.action == "create"
            && (schedules == 0 || input.name.is_none() || input.prompt.is_none()))
    {
        return bad("Give a name, a prompt, and one schedule: --cron, --at, or --in");
    }
    Ok(())
}

fn flag<'a>(words: &[&'a str], name: &str) -> Option<&'a str> {
    words
        .iter()
        .position(|w| *w == name)
        .and_then(|i| words.get(i + 1).copied())
}

/// `--in 30m` as a PostgreSQL interval.
fn delay(value: &str) -> Option<String> {
    let split = value.find(|c: char| !c.is_ascii_digit())?;
    let (count, unit) = value.split_at(split);
    let unit = match unit {
        "s" => "seconds",
        "m" => "minutes",
        "h" => "hours",
        "d" => "days",
        _ => return None,
    };
    (count.parse::<u32>().ok()? > 0).then(|| format!("{count} {unit}"))
}

/// `cloudroom automation ...`, the cloud twin of `room-cli automation create --cloud`.
pub async fn cli(args: &[String]) -> io::Result<i32> {
    let words: Vec<&str> = args
        .iter()
        .map(String::as_str)
        .filter(|w| *w != "--json")
        .collect();
    if words.is_empty() || words.iter().any(|w| matches!(*w, "--help" | "-h")) {
        println!("{USAGE}");
        return Ok(0);
    }
    let mut body = json!({"action": words[0]});
    match words.as_slice() {
        ["list"] => {}
        ["pause" | "resume" | "run" | "delete", id] => body["id"] = json!(id),
        ["create", ..] | ["update", ..] => {
            if words[0] == "update" {
                let Some(id) = words.get(1).filter(|w| !w.starts_with('-')) else {
                    println!("{USAGE}");
                    return Ok(2);
                };
                body["id"] = json!(id);
            }
            let prompt = match (flag(&words, "--prompt"), flag(&words, "--prompt-file")) {
                (Some(text), None) => Some(text.to_owned()),
                (None, Some(path)) => Some(std::fs::read_to_string(path)?),
                (None, None) => None,
                _ => {
                    println!("{USAGE}");
                    return Ok(2);
                }
            };
            if let Some(prompt) = prompt {
                body["prompt"] = json!(prompt);
            }
            for (name, key) in [
                ("--name", "name"),
                ("--cron", "cron"),
                ("--timezone", "timezone"),
                ("--at", "at"),
            ] {
                if let Some(value) = flag(&words, name) {
                    body[key] = json!(value);
                }
            }
            if let Some(value) = flag(&words, "--in") {
                let Some(interval) = delay(value) else {
                    eprintln!("Use --in with a number and s, m, h or d, like 30m.");
                    return Ok(2);
                };
                body["delay"] = json!(interval);
            }
        }
        _ => {
            println!("{USAGE}");
            return Ok(2);
        }
    }
    let answer = crate::mac::request(crate::mac::SOCKET, "POST", "/automation", Some(body)).await?;
    println!("{answer}");
    Ok(0)
}
