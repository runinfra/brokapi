//! Log files: a writer that starts a new file at a size, and the reading of them for `brokapi logs`.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::config::LogPolicy;

/// A log file that is closed at `max_bytes` and renamed to `<name>.1`, the older ones to `.2`, `.3` ... up to `keep`.
pub struct RotatingFile {
    path: PathBuf,
    max_bytes: u64,
    keep: usize,
    file: File,
    size: u64,
}

impl RotatingFile {
    /// Opens the file for appending (it is made, with its directory, if it is not there).
    pub fn open(path: &Path, policy: &LogPolicy) -> std::io::Result<Self> {
        Self::open_with(
            path,
            policy.max_size_mb.saturating_mul(1024 * 1024),
            policy.keep,
        )
    }

    /// [`RotatingFile::open`] with the size in bytes.
    pub fn open_with(path: &Path, max_bytes: u64, keep: usize) -> std::io::Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            path: path.to_owned(),
            max_bytes: max_bytes.max(1),
            keep,
            file,
            size,
        })
    }

    fn numbered(&self, n: usize) -> PathBuf {
        let mut name = self.path.clone().into_os_string();
        name.push(format!(".{n}"));
        PathBuf::from(name)
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        if self.keep == 0 {
            std::fs::remove_file(&self.path)?;
        } else {
            let _ = std::fs::remove_file(self.numbered(self.keep));
            for n in (1..self.keep).rev() {
                let from = self.numbered(n);
                if from.exists() {
                    std::fs::rename(&from, self.numbered(n + 1))?;
                }
            }
            std::fs::rename(&self.path, self.numbered(1))?;
        }
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.size = 0;
        Ok(())
    }
}

impl Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.size > 0 && self.size + buf.len() as u64 > self.max_bytes {
            self.rotate()?;
        }
        let n = self.file.write(buf)?;
        self.size += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Where a program writes its log: a file, the error output, or both.
#[derive(Clone)]
struct Sink {
    file: Option<Arc<Mutex<RotatingFile>>>,
    stderr: bool,
}

struct SinkWriter(Sink);

impl Write for SinkWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.0.stderr {
            let _ = std::io::stderr().write_all(buf);
        }
        if let Some(file) = &self.0.file {
            file.lock()
                .unwrap_or_else(|e| e.into_inner())
                .write_all(buf)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(file) = &self.0.file {
            file.lock().unwrap_or_else(|e| e.into_inner()).flush()?;
        }
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Sink {
    type Writer = SinkWriter;

    fn make_writer(&'a self) -> SinkWriter {
        SinkWriter(self.clone())
    }
}

/// The level from the variable `BROKAPI_LOG` (`error`, `warn`, `info`, `debug`, `trace`), `info` if it is not set.
fn level() -> tracing_subscriber::filter::LevelFilter {
    std::env::var("BROKAPI_LOG")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(tracing_subscriber::filter::LevelFilter::INFO)
}

/// Sets up the log of this process. Panics are written to it too, so a gateway that dies of one leaves the reason.
/// Called once; a second call does nothing.
pub fn init(file: Option<(&Path, &LogPolicy)>, stderr: bool) -> std::io::Result<()> {
    let file = match file {
        Some((path, policy)) => Some(Arc::new(Mutex::new(RotatingFile::open(path, policy)?))),
        None => None,
    };
    let sink = Sink { file, stderr };
    let set = tracing_subscriber::fmt()
        .with_writer(sink)
        .with_max_level(level())
        .with_target(false)
        .with_ansi(false)
        .try_init();
    if set.is_ok() {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            tracing::error!("panic: {info}");
            previous(info);
        }));
    }
    Ok(())
}

/// The last `n` lines of a file (fewer if it is shorter). A file that is not there has none.
pub fn tail_lines(path: &Path, n: usize) -> std::io::Result<Vec<String>> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    // Read backwards in blocks until there are enough line breaks.
    let len = file.metadata()?.len();
    let mut start = len;
    let mut data = Vec::new();
    while start > 0 && data.iter().filter(|b| **b == b'\n').count() <= n {
        let step = start.min(8192);
        start -= step;
        file.seek(SeekFrom::Start(start))?;
        let mut block = vec![0u8; step as usize];
        file.read_exact(&mut block)?;
        block.extend_from_slice(&data);
        data = block;
    }
    let text = String::from_utf8_lossy(&data);
    let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
    if start > 0 && !lines.is_empty() {
        // The first line is cut in the middle.
        lines.remove(0);
    }
    let skip = lines.len().saturating_sub(n);
    Ok(lines.split_off(skip))
}

/// Reads what is added to a log file after a position. It follows the file when it is rotated.
pub struct Follower {
    path: PathBuf,
    pos: u64,
}

impl Follower {
    /// Starts at the end of the file now.
    pub fn at_end(path: &Path) -> Self {
        let pos = std::fs::metadata(path).map_or(0, |m| m.len());
        Self {
            path: path.to_owned(),
            pos,
        }
    }

    /// The complete lines added since the last call.
    pub fn poll(&mut self) -> std::io::Result<Vec<String>> {
        let Ok(mut file) = File::open(&self.path) else {
            return Ok(Vec::new());
        };
        let len = file.metadata()?.len();
        if len < self.pos {
            // A new file replaced the one we were reading.
            self.pos = 0;
        }
        if len == self.pos {
            return Ok(Vec::new());
        }
        file.seek(SeekFrom::Start(self.pos))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        // Only whole lines: a half written one is read next time.
        let Some(end) = data.iter().rposition(|b| *b == b'\n') else {
            return Ok(Vec::new());
        };
        self.pos += end as u64 + 1;
        Ok(String::from_utf8_lossy(&data[..end])
            .lines()
            .map(str::to_owned)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_file_is_renamed_and_the_oldest_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.log");
        let mut log = RotatingFile::open_with(&path, 10, 2).unwrap();
        for word in ["aaaaaa\n", "bbbbbb\n", "cccccc\n", "dddddd\n"] {
            log.write_all(word.as_bytes()).unwrap();
        }
        let read = |p: &Path| std::fs::read_to_string(p).unwrap();
        assert_eq!(read(&path), "dddddd\n");
        assert_eq!(read(&dir.path().join("x.log.1")), "cccccc\n");
        assert_eq!(read(&dir.path().join("x.log.2")), "bbbbbb\n");
        assert!(!dir.path().join("x.log.3").exists());
    }

    #[test]
    fn tail_and_follow() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.log");
        assert!(tail_lines(&path, 3).unwrap().is_empty());
        let text: String = (1..=20_000).map(|i| format!("line {i}\n")).collect();
        std::fs::write(&path, &text).unwrap();
        assert_eq!(tail_lines(&path, 2).unwrap(), ["line 19999", "line 20000"]);
        assert_eq!(tail_lines(&path, 100_000).unwrap().len(), 20_000);
        let mut follow = Follower::at_end(&path);
        assert!(follow.poll().unwrap().is_empty());
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"new one\nhalf").unwrap();
        assert_eq!(follow.poll().unwrap(), ["new one"]);
        f.write_all(b" a line\n").unwrap();
        assert_eq!(follow.poll().unwrap(), ["half a line"]);
        // The file is replaced by a shorter one (a rotation).
        std::fs::write(&path, "fresh\n").unwrap();
        assert_eq!(follow.poll().unwrap(), ["fresh"]);
    }
}
