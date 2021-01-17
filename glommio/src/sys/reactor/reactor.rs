use std::cell::RefCell;
use std::io;
use std::os::unix::prelude::RawFd;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use rlimit::Resource;

use crate::sys::op::{Operation, UnsubmittedOp};
use crate::sys::reactor::{Rings, Unparker};
use crate::sys::uring::UringBufferAllocator;
use crate::sys::{DmaBuffer, PollableStatus, Source};

use super::{ParkMode, QueuedOp, TurnStats};

/// io_uring backed reactor
pub(crate) struct Reactor {
    rings: RefCell<Rings>,
    allocator: Rc<UringBufferAllocator>,
}

impl Reactor {
    pub(crate) fn new(min_io_memory: usize) -> io::Result<Self> {
        let mut io_memory = min_io_memory;
        const MIN_MEMLOCK_LIMIT: u64 = 512 * 1024;
        let (memlock_limit, _) = Resource::MEMLOCK.get()?;
        if memlock_limit < MIN_MEMLOCK_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "The memlock resource limit is too low: {} (recommended {})",
                    memlock_limit, MIN_MEMLOCK_LIMIT
                ),
            ));
        }
        // always have at least some small amount of memory for the slab
        io_memory = std::cmp::max(align_up(io_memory, 4096), 65536);
        let allocator = UringBufferAllocator::new(io_memory);
        let allocator = Rc::new(allocator);
        let registry = vec![io::IoSlice::new(allocator.as_bytes())];

        let rings = Rings::new(Rc::clone(&allocator), &registry)?;
        let rings = RefCell::new(rings);

        Ok(Reactor { rings, allocator })
    }

    pub(crate) fn unparker(&self) -> Arc<Unparker> {
        self.rings.borrow().unparker()
    }

    pub(crate) fn park(&self, mode: ParkMode) -> io::Result<TurnStats> {
        self.rings.borrow_mut().park(mode)
    }

    pub(crate) fn needs_preempt(&self) -> bool {
        self.rings.borrow_mut().needs_preempt()
    }

    pub(crate) fn eventfd(&self) -> Arc<AtomicUsize> {
        todo!("get eventfd storage")
    }

    pub(crate) fn alloc_dma_buffer(&self, size: usize) -> DmaBuffer {
        todo!("alloc dma buffers")
    }

    pub(crate) fn write_dma(
        &self,
        raw: RawFd,
        buf: DmaBuffer,
        pos: u64,
        pollable: PollableStatus,
    ) -> Source {
        todo!("write dma support")
    }

    pub(crate) fn write_buffered(&self, raw: RawFd, buf: Vec<u8>, pos: u64) -> Source {
        todo!("write buffered support")
    }

    pub(crate) fn connect(&self, raw: RawFd, addr: iou::SockAddr) -> Source {
        todo!("connect support")
    }

    pub(crate) fn accept(&self, raw: RawFd) -> Source {
        todo!("accept support")
    }

    pub(crate) fn send(&self, fd: RawFd, buf: DmaBuffer) -> Source {
        todo!("rushed send support")
    }

    pub(crate) fn sendmsg(
        &self,
        fd: RawFd,
        buf: DmaBuffer,
        addr: nix::sys::socket::SockAddr,
    ) -> io::Result<Source> {
        todo!("support sendmsg")
    }

    pub(crate) fn recvmsg(
        &self,
        fd: RawFd,
        size: usize,
        flags: iou::MsgFlags,
    ) -> io::Result<Source> {
        todo!("recvmsg")
    }

    pub(crate) fn recv(&self, fd: RawFd, size: usize, flags: iou::MsgFlags) -> Source {
        todo!("recv")
    }

    pub(crate) fn read_dma(
        &self,
        raw: RawFd,
        pos: u64,
        size: usize,
        pollable: PollableStatus,
    ) -> Source {
        todo!("read_dma")
    }

    pub(crate) fn read_buffered(&self, raw: RawFd, pos: u64, size: usize) -> Source {
        todo!("read_buffered")
    }

    pub(crate) fn fdatasync(&self, raw: RawFd) -> Source {
        todo!("fdatasync")
    }

    pub(crate) fn fallocate(
        &self,
        raw: RawFd,
        position: u64,
        size: u64,
        flags: libc::c_int,
    ) -> Source {
        todo!("fallocate")
    }

    pub(crate) fn close(&self, raw: RawFd) -> Source {
        todo!("close")
    }

    pub(crate) fn statx(&self, raw: RawFd, path: &Path) -> Source {
        todo!("statx")
    }

    pub(crate) fn open_at(
        &self,
        dir: RawFd,
        path: &Path,
        flags: libc::c_int,
        mode: libc::c_int,
    ) -> Source {
        todo!("open_at")
    }

    pub(crate) fn noop(&self) -> QueuedOp {
        let mut rings = self.rings.borrow_mut();
        let main_ring = rings.main_ring_mut();
        let op = UnsubmittedOp::new(Operation::new_noop());
        main_ring.submit_op(op)
    }
}

fn align_up(v: usize, align: usize) -> usize {
    (v + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    #[test]
    fn test_turn_cross_thread_wakeup() {
        let reactor = Reactor::new(8096).unwrap();
        let unparker = reactor.unparker();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(1000));
            unparker.wake();
        });
        reactor
            .park(ParkMode::Timeout(Duration::from_secs(u64::MAX), None))
            .unwrap();
    }
}
