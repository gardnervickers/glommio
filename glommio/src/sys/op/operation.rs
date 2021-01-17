use crate::sys::op::Shared;
use crate::sys::IOBuffer;
use std::ffi::CString;
use std::fmt::{self, Debug};
use std::os::unix::io::RawFd;
use std::time::Duration;

/// Operations supported by the reactor.
#[derive(Debug)]
pub(crate) struct Operation {
    fd: Option<RawFd>,
    op: Descriptor,
}

impl Shared for Operation {
    unsafe fn prep_sqe(&mut self, sqe: &mut iou::SubmissionQueueEvent<'_>) {
        Operation::prep_sqe(self, sqe)
    }
}

enum Descriptor {
    Noop,
    PollAdd(iou::PollFlags),

    PollRemove(u64),

    Cancel(u64),

    Write {
        buf: Box<IOBuffer>,
        position: u64,
    },

    Read {
        buf: Box<IOBuffer>,
        position: u64,
    },

    OpenAt {
        path: CString,
        flags: i32,
        mode: iou::OpenMode,
    },

    FDataSync,

    Fallocate {
        offset: u64,
        size: u64,
        flags: iou::FallocateFlags,
    },

    Statx {
        path: CString,
        flags: iou::StatxFlags,
        storage: Box<libc::statx>,
    },

    Timeout {
        ts: uring_sys::__kernel_timespec,
    },

    TimeoutRemove(u64),

    Close,
}

impl Debug for Descriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Descriptor").finish()
    }
}

impl Operation {
    pub(crate) fn new_noop() -> Self {
        Self {
            fd: None,
            op: Descriptor::Noop,
        }
    }

    pub(crate) fn new_poll_add(fd: RawFd, flags: iou::PollFlags) -> Self {
        Self {
            fd: Some(fd),
            op: Descriptor::PollAdd(flags),
        }
    }

    pub(crate) fn new_poll_remove(id: u64) -> Self {
        Self {
            fd: None,
            op: Descriptor::PollRemove(id),
        }
    }

    pub(crate) fn new_cancel(id: u64) -> Self {
        Self {
            fd: None,
            op: Descriptor::Cancel(id),
        }
    }

    pub(crate) fn new_write(fd: RawFd, buf: Box<IOBuffer>, position: u64) -> Self {
        Self {
            fd: Some(fd),
            op: Descriptor::Write { buf, position },
        }
    }

    pub(crate) fn new_read(fd: RawFd, buf: Box<IOBuffer>, position: u64) -> Self {
        Self {
            fd: Some(fd),
            op: Descriptor::Read { buf, position },
        }
    }

    pub(crate) fn new_open_at(fd: RawFd, path: CString, flags: i32, mode: iou::OpenMode) -> Self {
        Self {
            fd: Some(fd),
            op: Descriptor::OpenAt { path, flags, mode },
        }
    }

    pub(crate) fn new_timeout(duration: Duration) -> Self {
        Self {
            fd: None,
            op: Descriptor::Timeout {
                ts: uring_sys::__kernel_timespec {
                    tv_sec: duration.as_secs() as i64,
                    tv_nsec: duration.subsec_nanos() as libc::c_longlong,
                },
            },
        }
    }

    pub(crate) unsafe fn prep_sqe(&mut self, sqe: &mut iou::SubmissionQueueEvent<'_>) {
        match self.op {
            Descriptor::Noop => sqe.prep_nop(),

            Descriptor::PollAdd(flags) => sqe.prep_poll_add(self.fd.unwrap(), flags),

            Descriptor::PollRemove(id) => sqe.prep_poll_remove(id),

            Descriptor::Cancel(id) => sqe.prep_cancel(id),

            Descriptor::Write {
                ref mut buf,
                position,
            } => {
                match buf.as_mut() {
                    IOBuffer::Dma(ref mut dmabuf) => {
                        if let Some(idx) = dmabuf.uring_buffer_id() {
                            sqe.prep_write_fixed(
                                self.fd.unwrap(),
                                dmabuf.as_bytes_mut(),
                                position,
                                idx,
                            )
                        } else {
                            sqe.prep_write(self.fd.unwrap(), dmabuf.as_bytes_mut(), position)
                        }
                    }
                    IOBuffer::Buffered(ref mut vec) => {
                        sqe.prep_write(self.fd.unwrap(), &vec[..], position)
                    }
                };
            }

            Descriptor::Read {
                ref mut buf,
                position,
            } => {
                match buf.as_mut() {
                    IOBuffer::Dma(ref mut dmabuf) => {
                        if let Some(idx) = dmabuf.uring_buffer_id() {
                            sqe.prep_read_fixed(
                                self.fd.unwrap(),
                                dmabuf.as_bytes_mut(),
                                position,
                                idx,
                            )
                        } else {
                            sqe.prep_read(self.fd.unwrap(), dmabuf.as_bytes_mut(), position)
                        }
                    }
                    IOBuffer::Buffered(ref mut vec) => {
                        sqe.prep_read(self.fd.unwrap(), &mut vec[..], position)
                    }
                };
            }
            Descriptor::OpenAt {
                ref mut path,
                flags,
                mode,
            } => sqe.prep_openat(self.fd.unwrap(), path.as_c_str(), flags, mode),

            Descriptor::FDataSync => {
                sqe.prep_fsync(self.fd.unwrap(), iou::FsyncFlags::FSYNC_DATASYNC)
            }
            Descriptor::Fallocate {
                offset,
                size,
                flags,
            } => sqe.prep_fallocate(self.fd.unwrap(), offset, size, flags),

            Descriptor::Statx {
                ref path,
                flags,
                ref mut storage,
            } => {
                let mode = iou::StatxMode::from_bits_truncate(0x7ff);

                sqe.prep_statx(self.fd.unwrap(), path.as_c_str(), flags, mode, storage)
            }

            Descriptor::Timeout { ref ts } => sqe.prep_timeout(&ts),

            Descriptor::TimeoutRemove(timer_id) => sqe.prep_timeout_remove(timer_id),

            Descriptor::Close => sqe.prep_close(self.fd.unwrap()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_size() {
        println!("size {}", std::mem::size_of::<Operation>());
    }
}
