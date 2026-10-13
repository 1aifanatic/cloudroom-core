use crate::config::Config;
use crate::workspace::storage::{Disk, disk};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    time::Instant,
};

/// Linux reports process CPU in 100 Hz ticks (USER_HZ) on every architecture we run on.
const MS_PER_TICK: u64 = 10;
/// A minute with no agent working that still used half a CPU is idle burn: a leftover job, or a bug.
const IDLE_BURN_MS: u64 = 30_000;
const TOP_PROGRAMS: usize = 5;

#[derive(Serialize)]
pub(crate) struct Resources {
    cpu_used_percent: Option<f64>,
    memory_total_bytes: Option<u64>,
    memory_used_bytes: Option<u64>,
    workspace_disk: Option<Disk>,
    state_disk: Option<Disk>,
    pub(super) agents: Option<super::metrics::AgentCounts>,
    /// Once a minute; absent from the other samples.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cpu_minute: Option<CpuMinute>,
}

/// Where this machine's (or container's) CPU went in the last minute. Program names only, never arguments or paths.
#[derive(Serialize)]
pub(crate) struct CpuMinute {
    total_ms: u64,
    /// The top programs, `(exited)` for processes that ended during the minute, and `(other)` for the rest.
    programs: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    idle_burn: bool,
}

/// One process's CPU ticks: its own, and its finished children's (added when it reaps them).
#[derive(Clone, Default)]
struct Ticks {
    program: String,
    own: u64,
    children: u64,
}
/// Keyed by pid and start time, because pids are reused.
type Processes = HashMap<(u32, u64), Ticks>;

impl Resources {
    pub(super) fn dashboard(&self) -> Value {
        let percent = |used: Option<u64>, total: Option<u64>| {
            used.zip(total)
                .filter(|(used, total)| *total > 0 && used <= total)
                .map(|(used, total)| used as f64 * 100.0 / total as f64)
        };
        json!({"cpu": self.cpu_used_percent,
            "memory": percent(self.memory_used_bytes, self.memory_total_bytes),
            "disk": self.workspace_disk.as_ref().and_then(|d| percent(Some(d.used_bytes), d.used_bytes.checked_add(d.available_bytes))),
            "memoryUsedBytes": self.memory_used_bytes, "memoryTotalBytes": self.memory_total_bytes,
            "diskAvailableBytes": self.workspace_disk.as_ref().map(|d| d.available_bytes),
            "diskTotalBytes": self.workspace_disk.as_ref().map(|d| d.total_bytes)})
    }
}

#[derive(Default)]
pub(super) struct Sampler {
    cpu: Option<(u64, u64)>,
    container_cpu: Option<(u64, Instant)>,
    processes: Option<Processes>,
}

impl Sampler {
    /// Inside a container, `/proc/stat` and `/proc/meminfo` describe the whole host, so the container's own
    /// cgroup counters win when the provider exposes them.
    pub async fn sample(&mut self, config: &Config) -> Resources {
        let current = fs::read_to_string("/proc/stat").ok().and_then(|s| cpu(&s));
        let mut cpu_used_percent =
            current
                .zip(self.cpu)
                .and_then(|((total, idle), (old_total, old_idle))| {
                    let total = total.checked_sub(old_total)?;
                    let idle = idle.checked_sub(old_idle)?;
                    (total > 0 && idle <= total)
                        .then(|| (total - idle) as f64 * 100.0 / total as f64)
                });
        self.cpu = current;
        let usage = cgroup("cpu.stat").and_then(|s| field(&s, "usage_usec"));
        if let Some((usage, cpus)) = usage.zip(cgroup_cpus()) {
            let now = Instant::now();
            cpu_used_percent = self.container_cpu.and_then(|(old, at)| {
                let wall = now.duration_since(at).as_micros() as f64 * cpus;
                (wall > 0.0).then(|| usage.saturating_sub(old) as f64 * 100.0 / wall)
            });
            self.container_cpu = Some((usage, now));
        }
        let memory = fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let mut memory_total_bytes = memory_field(&memory, "MemTotal:");
        let mut memory_used_bytes = memory_total_bytes
            .zip(memory_field(&memory, "MemAvailable:"))
            .and_then(|(total, available)| total.checked_sub(available));
        let limit = cgroup("memory.max").and_then(|s| s.trim().parse::<u64>().ok());
        if let Some((limit, current)) = limit.zip(cgroup("memory.current")) {
            // Like `docker stats`: reclaimable file cache is not in use.
            let inactive = cgroup("memory.stat").and_then(|s| field(&s, "inactive_file"));
            memory_total_bytes = Some(limit);
            memory_used_bytes = current
                .trim()
                .parse::<u64>()
                .ok()
                .map(|used| used.saturating_sub(inactive.unwrap_or(0)));
        }
        Resources {
            cpu_used_percent,
            memory_total_bytes,
            memory_used_bytes,
            workspace_disk: disk(&config.repository).ok(),
            state_disk: disk(&config.state_dir).ok(),
            agents: None,
            cpu_minute: None,
        }
    }

    /// CPU per program since the last call, from every process `/proc` shows. Called once a minute; the first
    /// call only takes a baseline. `busy`: an agent worked or waited in line at some point during the minute.
    pub fn minute(&mut self, busy: bool) -> Option<CpuMinute> {
        let now = processes();
        let before = self.processes.replace(now)?;
        let (mut used, exited) = account(&before, self.processes.as_ref()?);
        used.sort_by_key(|(_, ticks)| std::cmp::Reverse(*ticks));
        let total_ms = (used.iter().map(|(_, ticks)| ticks).sum::<u64>() + exited) * MS_PER_TICK;
        let mut programs = BTreeMap::new();
        for (i, (program, ticks)) in used.into_iter().enumerate() {
            let name = if i < TOP_PROGRAMS {
                program
            } else {
                "(other)".into()
            };
            *programs.entry(name).or_default() += ticks * MS_PER_TICK;
        }
        if exited > 0 {
            programs.insert("(exited)".into(), exited * MS_PER_TICK);
        }
        Some(CpuMinute {
            total_ms,
            programs,
            idle_burn: !busy && total_ms >= IDLE_BURN_MS,
        })
    }
}

/// Ticks per program between two readings, and ticks of processes that ended in between. An ended process's
/// whole count moves into its reaper's `children`, so only its part since the last reading is new.
fn account(before: &Processes, now: &Processes) -> (Vec<(String, u64)>, u64) {
    let mut used: HashMap<String, u64> = HashMap::new();
    let mut exited: i128 = 0;
    for (key, process) in now {
        let old = before.get(key).cloned().unwrap_or_default();
        *used.entry(process.program.clone()).or_default() += process.own.saturating_sub(old.own);
        exited += i128::from(process.children.saturating_sub(old.children));
    }
    for (key, process) in before {
        if !now.contains_key(key) {
            exited -= i128::from(process.own + process.children);
        }
    }
    let used = used.into_iter().filter(|(_, ticks)| *ticks > 0).collect();
    (used, exited.max(0) as u64)
}

fn processes() -> Processes {
    let mut found = Processes::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return found;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let Some((head, rest)) = stat.rsplit_once(") ") else {
            continue;
        };
        // proc(5) fields 14-17 are utime, stime, cutime, cstime; 22 is the start time. `rest` starts at field 3.
        let fields: Vec<&str> = rest.split_whitespace().collect();
        let tick = |field: usize| fields.get(field - 3).and_then(|v| v.parse::<i64>().ok());
        let (Some(utime), Some(stime), Some(cutime), Some(cstime), Some(start)) =
            (tick(14), tick(15), tick(16), tick(17), tick(22))
        else {
            continue;
        };
        let comm = head.split_once(" (").map_or("", |(_, comm)| comm);
        let ticks = Ticks {
            program: program(pid, comm),
            own: (utime + stime).max(0) as u64,
            children: (cutime + cstime).max(0) as u64,
        };
        found.insert((pid, start.max(0) as u64), ticks);
    }
    found
}

/// The process name; for Node, the npm package it runs (`typescript`, `@anthropic-ai/claude-code`), so tools and
/// harnesses don't all read `node`.
fn program(pid: u32, comm: &str) -> String {
    if comm != "node" {
        return comm.to_owned();
    }
    let cmdline = fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
    let cmdline = String::from_utf8_lossy(&cmdline);
    let package = cmdline.split('\0').find_map(|arg| {
        let mut parts = arg.rsplit_once("node_modules/")?.1.split('/');
        let first = parts.next().filter(|name| !name.is_empty())?;
        if first.starts_with('@') {
            Some(format!("{first}/{}", parts.next()?))
        } else {
            Some(first.to_owned())
        }
    });
    package.unwrap_or_else(|| comm.to_owned())
}

/// The container's own cgroup v2 counters, when visible.
fn cgroup(file: &str) -> Option<String> {
    fs::read_to_string(format!("/sys/fs/cgroup/{file}")).ok()
}

/// CPUs the container may use: its quota, or else every CPU this process can run on. None outside a container
/// (the root cgroup has no `cpu.max`), where `/proc/stat` is already right.
fn cgroup_cpus() -> Option<f64> {
    let limit = cgroup("cpu.max")?;
    let mut parts = limit.split_whitespace();
    let quota = parts.next()?.parse::<f64>().ok();
    let period = parts.next()?.parse::<f64>().ok()?;
    quota.map(|quota| quota / period).or_else(|| {
        std::thread::available_parallelism()
            .ok()
            .map(|n| n.get() as f64)
    })
}

fn field(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        line.strip_prefix(name)?
            .strip_prefix(' ')?
            .trim()
            .parse()
            .ok()
    })
}

#[test]
fn disk_percentage_excludes_reserved_blocks() {
    let mut resources = Resources {
        cpu_used_percent: None,
        memory_total_bytes: None,
        memory_used_bytes: None,
        workspace_disk: Some(Disk {
            total_bytes: 80,
            used_bytes: 77,
            available_bytes: 0,
        }),
        state_disk: None,
        agents: None,
        cpu_minute: None,
    };
    assert_eq!(resources.dashboard()["disk"], 100.0);
    resources.workspace_disk = Some(Disk {
        total_bytes: 80,
        used_bytes: 38,
        available_bytes: 38,
    });
    assert_eq!(resources.dashboard()["disk"], 50.0);
    resources.workspace_disk = None;
    assert!(resources.dashboard()["disk"].is_null());
}

fn cpu(text: &str) -> Option<(u64, u64)> {
    let mut line = text.lines().next()?.split_whitespace();
    if line.next()? != "cpu" {
        return None;
    }
    // Guest time is already included in user/nice; do not count the last two fields twice.
    let counters: Vec<u64> = line
        .take(8)
        .map(str::parse)
        .collect::<Result<_, _>>()
        .ok()?;
    if counters.len() < 8 {
        return None;
    }
    Some((counters.iter().sum(), counters[3] + counters[4]))
}

fn memory_field(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        if fields.next()? != name {
            return None;
        }
        fields.next()?.parse::<u64>().ok()?.checked_mul(1024)
    })
}

#[test]
fn cpu_minute_counts_ended_processes_once() {
    let ticks = |program: &str, own, children| Ticks {
        program: program.into(),
        own,
        children,
    };
    // Before: a harness and a build it started.
    let before = Processes::from([
        ((1, 1), ticks("claude", 100, 0)),
        ((2, 5), ticks("tsc", 300, 0)),
    ]);
    // The build used 50 more ticks and ended, and a 20-tick command came and went; the harness reaped both.
    let now = Processes::from([
        ((1, 1), ticks("claude", 110, 370)),
        ((3, 9), ticks("cargo", 40, 0)),
    ]);
    let (mut used, exited) = account(&before, &now);
    used.sort();
    assert_eq!(used, [("cargo".to_owned(), 40), ("claude".to_owned(), 10)]);
    assert_eq!(exited, 70);
}

#[cfg(target_os = "linux")]
#[test]
fn cpu_minute_sees_real_processes() {
    let mut sampler = Sampler::default();
    assert!(
        sampler.minute(true).is_none(),
        "the first call is a baseline"
    );
    let start = Instant::now();
    while start.elapsed() < std::time::Duration::from_millis(500) {
        std::hint::black_box(start.elapsed());
    }
    let minute = sampler.minute(false).expect("a reading");
    assert!(minute.total_ms >= 200, "{} ms", minute.total_ms);
    assert!(minute.programs.len() <= TOP_PROGRAMS + 2);
    assert!(
        !minute.idle_burn,
        "half a second is under the idle-burn threshold"
    );
}
