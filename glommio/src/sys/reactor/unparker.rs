//! Unparker allows unparking an event loop from a remote thread.
//!
//! Glommio utilizes a thread-per-core architecture where cross thread synchronization is
//! kept to a minimum. Despite this, it's necessary to maintain a set of queues for message
//! passing between cores. Because each thread/core can sleep on the event loop at an arbitrary
//! time, we need a way to wake up threads/cores from other threads/cores.
//!
//! To satisfy this, each Glommio thread registers an eventfd read before sleeping on a ring. Other threads
//! can write to this eventfd to signal a wakeup.
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::sync::atomic::{self, Ordering};
use std::sync::Arc;

/// Bitfield used for coordinating parking/unparking.
///
/// - `1 << 0`: Indicates that the reactor is entering or has entered sleep and will poll the eventfd.
///             If a remote thread witnesses this, an eventfd write is necessary.
///             This bit will only be set by the reactor.
/// - `1 << 1`: Indicates that a remote thread has requested that the reactor wake up. The remote thread which
///             successfully sets this bit is responsible for writing to the eventfd.
///
///
#[derive(Copy, Clone)]
pub(crate) struct UnparkerState(u8);
impl UnparkerState {
    /// unparked returns true if the Unparker has been requested to be woken.
    pub(crate) fn woken(self) -> bool {
        self.0 & Unparker::REMOTE_THREAD_BIT == Unparker::REMOTE_THREAD_BIT
    }

    /// is_park returns true if the Unparker is currently "parked" waiting on and eventfd update.
    pub(crate) fn is_parked(self) -> bool {
        self.0 & Unparker::REACTOR_PARK_BIT == Unparker::REACTOR_PARK_BIT
    }
}

impl fmt::Debug for UnparkerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnparkerState")
            .field("woken", &self.woken())
            .field("is_parked", &self.is_parked())
            .field("bits", &format!("0b{:08b}", self.0))
            .finish()
    }
}

/// Unparker contains an eventfd instance which can wakeup the driver.
#[derive(Debug)]
pub struct Unparker {
    flag: atomic::AtomicU8,
    fd: File,
}

impl Unparker {
    /// Set when the reactor is entering park, and unset once the eventfd poll has been seen.
    const REACTOR_PARK_BIT: u8 = 1 << 0;
    /// Set when a remote thread requests that the reactor owning this Unparker should unpark.
    const REMOTE_THREAD_BIT: u8 = 1 << 1;

    pub(crate) fn new() -> io::Result<Unparker> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(Unparker {
            flag: atomic::AtomicU8::new(0x00),
            fd: unsafe { FromRawFd::from_raw_fd(fd) },
        })
    }

    pub(crate) fn state(&self) -> UnparkerState {
        UnparkerState(self.flag.load(Ordering::Acquire))
    }

    /// Mark this unparker as "parked". This will signal to future wakers that a write to the eventfd is needed.
    ///
    /// The reactor should look at the returned UnparkerState to determine if there was a signal to wake.
    pub(crate) fn park(&self) -> UnparkerState {
        let state = self.flag.fetch_or(Self::REACTOR_PARK_BIT, Ordering::AcqRel);
        UnparkerState(state)
    }

    /// Reset this unparker. This should be called to clear the parking status from the reactor.
    pub(crate) fn reset(&self) {
        self.flag.fetch_and(
            !Self::REACTOR_PARK_BIT & !Self::REMOTE_THREAD_BIT,
            Ordering::Release,
        );
    }

    /// Wake this unparker from a remote thread.
    pub(crate) fn wake(self: &Arc<Self>) {
        let flag = &self.flag;
        let fd = &self.fd;
        // First, try to set the remote thread bit to signal the wakeup.
        let state = flag.fetch_or(Self::REMOTE_THREAD_BIT, Ordering::AcqRel);
        let state = UnparkerState(state);
        // Perform two checks, the first to see if the reactor is watching the eventfd, the second to see if we
        // were the thread which set the REMOTE_THREAD_BIT. If we did set the REMOTE_THREAD_BIT, we need to write to the eventfd.
        if !state.woken() && state.is_parked() {
            let _ = (fd as &File).write(&0x1u64.to_ne_bytes());
        }
    }
}

impl AsRawFd for Unparker {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_park_wake() {
        let unparker = Unparker::new().unwrap();
        let unparker = Arc::new(unparker);

        let old_state = unparker.park();
        assert!(!old_state.is_parked());
        assert!(!old_state.woken());

        assert!(unparker.state().is_parked());
        assert!(!unparker.state().woken());

        unparker.wake();
        assert!(unparker.state().is_parked());
        assert!(unparker.state().woken());

        unparker.reset();
        assert!(!unparker.state().is_parked());
        assert!(!unparker.state().woken());
    }
}
