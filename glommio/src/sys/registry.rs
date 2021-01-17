//! Track file descriptors which are shared between the kernal and the application.
//!
//! Completion-based IO requires that the ownership of resources is shared between the kernel and
//! the application. Because a Task can be dropped while a request utilizing a shared resource can be
//! dropped at any time, it's necessary to enqueue drops until the kernel has also "lost" interest in a resource.
use std::cell::{Cell, RefCell};
use std::fmt::{self, Debug};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::rc::Rc;

#[derive(Debug, Copy, Clone)]
enum FileDescriptor {
    Owned(RawFd),
    Registered { fd: RawFd, idx: usize },
}

/// Registry is a thread-local manager for files and sockets which are shared between the kernel and the application.
/// The primary use of the registry is to track outstanding interest in FD's and sockets, so that they can be closed
/// once the kernel is done with them.
pub struct Registry {
    state: Rc<RefCell<State>>,
}

impl Registry {
    pub fn new() -> Self {
        let state = State { freed_fds: vec![] };
        let state = RefCell::new(state);
        let state = Rc::new(state);
        Self { state }
    }

    /// Register a new file or socket. The returned `Registered<T>` can be used to construct requests to the kernel.
    pub(crate) fn register_fd(&self, resource: RawFd) -> RegisteredFileDescriptor {
        let registry = Rc::clone(&self.state);
        RegisteredFileDescriptor {
            fd: FileDescriptor::Owned(resource),
            refcnt: Rc::new(Cell::new(1)),
            registry,
        }
    }

    pub(crate) fn next_dropped_fd(&self) -> Option<RawFd> {
        let mut state = self.state.borrow_mut();
        state.freed_fds.pop().and_then(|v| match v {
            FileDescriptor::Owned(fd) => Some(fd),
            FileDescriptor::Registered { fd, .. } => Some(fd),
        })
    }

    pub(crate) fn num_freed(&self) -> usize {
        self.state.borrow().freed_fds.len()
    }
}

/// A [`RegisteredFileDescriptor`] allows Glommio to track file descriptor usage between the application
/// and the backing reactor.
pub struct RegisteredFileDescriptor {
    fd: FileDescriptor,
    refcnt: Rc<Cell<usize>>,
    registry: Rc<RefCell<State>>,
}

impl Debug for RegisteredFileDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisteredFileDescriptor")
            .field("fd", &self.fd)
            .finish()
    }
}

impl Clone for RegisteredFileDescriptor {
    fn clone(&self) -> Self {
        let fd = self.fd;
        let refcnt = Rc::clone(&self.refcnt);
        refcnt.set(refcnt.get() + 1);
        let registry = Rc::clone(&self.registry);
        RegisteredFileDescriptor {
            fd,
            refcnt,
            registry,
        }
    }
}

impl RegisteredFileDescriptor {
    /// Prepare an `SQE` for submission using this `Registered<FileDescriptor>`.
    ///
    /// This abstracts over both owned and registered files. The reference count for
    /// this `Registered<FileDescriptor>` will be bumped.
    pub(crate) fn prep_sqe(&self, sqe: &mut iou::SubmissionQueueEvent<'_>) {
        self.refcnt.set(self.refcnt.get() + 1);
        match self.fd {
            FileDescriptor::Owned(fd) => {
                sqe.raw_mut().fd = fd as RawFd;
            }
            FileDescriptor::Registered { fd, idx } => {
                sqe.raw_mut().fd = idx as RawFd;
                sqe.set_flags(iou::SubmissionFlags::FIXED_FILE);
            }
        }
    }
}

impl Drop for RegisteredFileDescriptor {
    fn drop(&mut self) {
        let old = self.refcnt.get();
        debug_assert_ne!(old, 0);
        self.refcnt.set(old - 1);
        // Signal the the registry that nothing has interest in this FD anymore.
        if old == 1 {
            let mut state = self.registry.borrow_mut();
            state.freed_fds.push(self.fd);
        }
    }
}

struct State {
    freed_fds: Vec<FileDescriptor>,
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use super::*;

    #[test]
    fn test_pop_tracked() {
        let registry = Registry::new();

        let fd1 = registry.register_fd(1.try_into().unwrap());
        let fd2 = registry.register_fd(2.try_into().unwrap());
        assert_eq!(0, registry.num_freed());

        drop(fd2);
        assert_eq!(1, registry.num_freed());
        assert_eq!(Some(2), registry.next_dropped_fd());

        let _fd1_cloned = fd1.clone();
        drop(fd1);
        assert_eq!(0, registry.num_freed());
    }
}
