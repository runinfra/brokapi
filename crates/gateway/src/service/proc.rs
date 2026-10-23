//! Processes: whether one lives, how much it uses, how to end it, and how to start one that outlives its parent.

use std::collections::HashMap;
use std::path::Path;

use sysinfo::{Pid, ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System};

/// Whether a process with this id exists and is not a zombie.
pub fn is_alive(pid: u32) -> bool {
    let mut sys = System::new();
    let pid = Pid::from_u32(pid);
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    sys.process(pid)
        .is_some_and(|p| !matches!(p.status(), ProcessStatus::Zombie | ProcessStatus::Dead))
}

/// Whether the process is a running `brokapi`. A pid in a file that outlived a reboot may belong to anything.
pub fn is_brokapi(pid: u32) -> bool {
    let mut sys = System::new();
    let pid = Pid::from_u32(pid);
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_exe(sysinfo::UpdateKind::OnlyIfNotSet),
    );
    let Some(p) = sys.process(pid) else {
        return false;
    };
    if matches!(p.status(), ProcessStatus::Zombie | ProcessStatus::Dead) {
        return false;
    }
    let name = p.name().to_string_lossy().to_ascii_lowercase();
    let exe = p
        .exe()
        .and_then(Path::file_stem)
        .map(|s| s.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    name.trim_end_matches(".exe") == "brokapi" || exe == "brokapi"
}

/// Asks the process to stop: SIGTERM on Unix. Windows has no such request for a process without a window, so the
/// process is ended (a gateway keeps nothing that its journal and the next start cannot recover).
pub fn terminate(pid: u32) -> bool {
    let mut sys = System::new();
    let pid = Pid::from_u32(pid);
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    let Some(p) = sys.process(pid) else {
        return false;
    };
    p.kill_with(sysinfo::Signal::Term)
        .unwrap_or_else(|| p.kill())
}

/// Ends the process at once.
pub fn kill(pid: u32) -> bool {
    let mut sys = System::new();
    let pid = Pid::from_u32(pid);
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    sys.process(pid).is_some_and(|p| p.kill())
}

/// Reads the CPU and the memory of processes. Keep one: the CPU is the change between two readings.
pub struct Usage {
    sys: System,
}

impl Default for Usage {
    fn default() -> Self {
        Self::new()
    }
}

impl Usage {
    /// A reader with no readings yet.
    pub fn new() -> Self {
        Self { sys: System::new() }
    }

    /// Reads these processes. Returns `pid -> (percent of one core, bytes of memory)`.
    pub fn read(&mut self, pids: &[u32]) -> HashMap<u32, (f32, u64)> {
        let list: Vec<Pid> = pids.iter().map(|p| Pid::from_u32(*p)).collect();
        self.sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&list),
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        pids.iter()
            .filter_map(|pid| {
                self.sys
                    .process(Pid::from_u32(*pid))
                    .map(|p| (*pid, (p.cpu_usage(), p.memory())))
            })
            .collect()
    }
}

/// Makes a command start a process that does not stop with its parent or with the terminal: its own process group on
/// Unix, no console and no group of the parent on Windows.
pub fn detach(cmd: &mut tokio::process::Command) {
    #[cfg(unix)]
    {
        cmd.process_group(0);
    }
    #[cfg(windows)]
    {
        // DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
        cmd.creation_flags(0x0000_0008 | 0x0000_0200 | 0x0800_0000);
        keep_std_handles();
    }
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(false);
}

/// Takes the handles of this process's input and output out of what a child inherits.
///
/// Windows hands a new process every handle that is marked inheritable, whatever the new process was told to use for its own
/// input and output. Whoever started this program with a pipe for its output (a script, a supervisor, a test) made the
/// handle of that pipe inheritable, so a service started in the background kept the pipe open: the reader waited for the
/// end of the output until the service stopped. Only the handles of the standard streams are cleared, in this process, which
/// is the short-lived `brokapi up -d`; the child gets the null device for its own streams.
#[cfg(windows)]
fn keep_std_handles() {
    use std::ffi::c_void;
    // `kernel32` is linked by the standard library already.
    unsafe extern "system" {
        fn GetStdHandle(which: u32) -> *mut c_void;
        fn SetHandleInformation(handle: *mut c_void, mask: u32, flags: u32) -> i32;
    }
    const HANDLE_FLAG_INHERIT: u32 = 0x1;
    // STD_INPUT_HANDLE, STD_OUTPUT_HANDLE and STD_ERROR_HANDLE: -10, -11 and -12 as the unsigned numbers the call takes.
    for which in [-10_i32, -11, -12] {
        // SAFETY: both calls only read or change a flag of a handle this process owns, and a handle that is missing (null)
        // or not valid (-1) is skipped. A failed call changes nothing, the result is not needed.
        unsafe {
            let handle = GetStdHandle(which as u32);
            if !handle.is_null() && handle as isize != -1 {
                SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0);
            }
        }
    }
}

/// Reads the pid in a pid file.
pub fn read_pid(path: &Path) -> Option<u32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_process_lives_and_is_not_brokapi() {
        assert!(is_alive(std::process::id()));
        assert!(
            !is_brokapi(std::process::id()),
            "a test binary is not named brokapi"
        );
        // No process has the largest pid.
        assert!(!is_alive(u32::MAX - 1));
    }

    #[test]
    fn the_usage_of_this_process_is_read() {
        let mut usage = Usage::new();
        let me = std::process::id();
        let read = usage.read(&[me, u32::MAX - 1]);
        assert!(read[&me].1 > 0, "a process has memory");
        assert!(!read.contains_key(&(u32::MAX - 1)));
    }
}
