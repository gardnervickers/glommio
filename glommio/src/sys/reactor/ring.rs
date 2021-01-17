#![allow(dead_code)]
use crate::sys::op::{complete_from_cqe, Operation, SubmittedOp, UnsubmittedOp};
use crate::sys::reactor::unparker::Unparker;
use crate::sys::reactor::{Notified, Notify};
use crate::sys::uring::UringBufferAllocator;
use crate::sys::TimeSpec64;

use std::cell::RefCell;
use std::future::Future;
use std::io::IoSlice;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::os::unix::prelude::{AsRawFd, RawFd};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use std::task::{Context, Poll};
use std::time::Duration;
use std::{cmp, io};

use pin_project_lite::pin_project;

struct InnerRing {
    ring: iou::IoUring,
    capacity: usize,
    polled: bool,
    enqueued: u64,
    submitted: u64,
    completed: u64,
    submission_control: Notify,
}

impl InnerRing {
    fn new(ring: iou::IoUring, capacity: usize, polled: bool) -> Self {
        let submission_control = Notify::new();
        Self {
            ring,
            capacity,
            polled,
            enqueued: 0,
            submitted: 0,
            completed: 0,
            submission_control,
        }
    }

    fn in_flight(&self) -> u64 {
        self.enqueued
            .checked_sub(self.completed)
            .expect("failed tracking ring in-flight") as u64
    }

    fn next_sqe(&mut self) -> Result<iou::SubmissionQueueEvent<'_>, Notified> {
        if let Some(sqe) = self.ring.next_sqe() {
            self.enqueued += 1;
            Ok(sqe)
        } else {
            Err(self.submission_control.wait())
        }
    }

    fn try_next_sqe(&mut self) -> Option<iou::SubmissionQueueEvent<'_>> {
        if let Some(sqe) = self.ring.next_sqe() {
            self.enqueued += 1;
            Some(sqe)
        } else {
            None
        }
    }

    // Submit SQEs and return immediately.
    fn submit_sqes(&mut self) -> io::Result<usize> {
        if self.enqueued > self.submitted {
            let submitted = self.ring.submit_sqes()?;
            self.submitted += submitted as u64;
            return Ok(submitted);
        } else {
            Ok(0)
        }
    }

    fn notify_wakers(&mut self) {
        let enqueued = self.enqueued.saturating_sub(self.submitted);
        let capacity = (self.capacity as u64).saturating_sub(enqueued);
        let amount = cmp::min(capacity as _, self.capacity - 4);
        self.submission_control.notify(amount);
    }

    fn submit_and_wait_timeout(&mut self, cnt: u32, duration: Duration) -> io::Result<usize> {
        self.enqueued += 1;
        let submitted = self.ring.submit_sqes_and_wait_with_timeout(cnt, duration)?;
        self.submitted += submitted as u64;
        return Ok(submitted);
    }

    fn submit_and_wait(&mut self, cnt: u32) -> io::Result<usize> {
        let submitted = self.ring.submit_sqes_and_wait(cnt)?;
        self.submitted += submitted as u64;
        return Ok(submitted);
    }

    fn drain_cqes(
        &mut self,
        preemption_timer: &mut ManuallyDrop<Option<TimeSpec64>>,
        ring_linked: &mut bool,
        unparker: &mut Option<Arc<Unparker>>,
    ) -> usize {
        if self.in_flight() == 0 {
            return 0;
        }
        let ring = &mut self.ring;
        let raw_ring = ring.raw_mut();
        let ready: u32 = unsafe { uring_sys::io_uring_cq_ready(raw_ring) };
        let mut to_drain = ready;
        while to_drain > 0 {
            let batch = cmp::min(to_drain, Ring::COMPLETION_BATCH_SIZE as _);
            let mut cqes: MaybeUninit<[*mut uring_sys::io_uring_cqe; Ring::COMPLETION_BATCH_SIZE]> =
                MaybeUninit::uninit();
            let peeked = unsafe {
                uring_sys::io_uring_peek_batch_cqe(raw_ring, cqes.as_mut_ptr() as _, batch)
            };
            let stack_cqes: [*mut uring_sys::io_uring_cqe; Ring::COMPLETION_BATCH_SIZE] =
                unsafe { cqes.assume_init() };

            for i in 0..peeked as usize {
                let cqe = stack_cqes[i];
                let cqe = unsafe {
                    iou::CompletionQueueEvent::from_raw((*cqe).user_data, (*cqe).res, (*cqe).flags)
                };
                if cqe.user_data() == uring_sys::LIBURING_UDATA_TIMEOUT {
                    // skip any timeouts we encounter by marking them ignored.
                    to_drain -= 1;
                } else if cqe.user_data() == Ring::PREEMPT_TIMER_TOKEN {
                    to_drain -= 1;
                    let old_preempt_timer_present = preemption_timer.take().is_some();
                    debug_assert!(old_preempt_timer_present);
                } else if cqe.user_data() == Ring::LINKED_RING_TOKEN {
                    to_drain -= 1;
                    *ring_linked = false;
                } else if cqe.user_data() == Ring::UNPARKER_WAKE_TOKEN {
                    unparker
                        .as_mut()
                        .map(|u| {
                            to_drain -= 1;
                            u.reset();
                        })
                        .expect("witnessed an unparker wake token on a ring with no unparker!");
                } else {
                    unsafe { complete_from_cqe::<Operation>(cqe) };
                    to_drain -= 1;
                }
            }
            unsafe { uring_sys::io_uring_cq_advance(raw_ring, peeked) };
        }
        let processed = ready - to_drain;
        self.completed += processed as u64;
        return processed as usize;
    }
}

/// Ring abstraction, representing the different types of rings controlled by the reactor.
pub(crate) struct Ring {
    ring: Rc<RefCell<InnerRing>>,
    capacity: usize,
    polled: bool,
    cqhead: *const u32,
    cqtail: *const u32,
    ring_fd: RawFd,
    name: &'static str,
    allocator: Rc<UringBufferAllocator>,
    unparker: Option<Arc<Unparker>>,
    unparker_buf: ManuallyDrop<Box<[u8; 8]>>,
    ring_linked: bool,
    preemption_timer: ManuallyDrop<Option<TimeSpec64>>,
}

#[derive(Debug)]
pub(crate) enum TurnMode {
    /// Turn this Ring and park on the next completion, or up to the provided timeout.
    Timeout(Duration),
    /// Turn this Ring and park indefinitely until at least one completion arrives.
    NextCompletion,
    /// Turn this Ring and return immediately.
    NoPark,
    /// Turn this Ring and park on the provided eventFd for up to the provided timeout.
    EventFDTimeout(RawFd, Duration),
    /// Turn this Ring and park for either a notification on the provided EventFD or at least one completion.
    EventFD(RawFd),
}

pub(crate) struct Turnt {
    completions: usize,
    submissions: usize,
    slept: bool,
}

impl Turnt {
    fn new() -> Turnt {
        Turnt {
            completions: 0,
            submissions: 0,
            slept: false,
        }
    }

    pub(crate) fn completed(&self) -> usize {
        self.completions
    }

    pub(crate) fn submitted(&self) -> usize {
        self.submissions
    }

    pub(crate) fn slept(&self) -> bool {
        self.slept
    }
}

impl Ring {
    /// Completions are copied to the stack in batches for efficiency. COMPLETION_BATCH_SIZE
    /// designates the size of the completion array.
    const COMPLETION_BATCH_SIZE: usize = 32;

    /// Token to use for the user_data field of the Unparker eventfd poll.
    const UNPARKER_WAKE_TOKEN: u64 = 0x01;

    /// Token to use for the user_data field of the linked ring FD poll.
    const LINKED_RING_TOKEN: u64 = 0x02;

    /// Token to use for the preemption timer.
    const PREEMPT_TIMER_TOKEN: u64 = 0x03;

    pub(crate) fn new(
        capacity: usize,
        name: &'static str,
        allocator: Rc<UringBufferAllocator>,
        polled: bool,
    ) -> io::Result<Self> {
        let mut ring = if polled {
            iou::IoUring::new_with_flags(capacity as _, iou::SetupFlags::IOPOLL)?
        } else {
            iou::IoUring::new_with_flags(capacity as _, iou::SetupFlags::empty())?
        };

        let raw_ring = ring.raw_mut();

        let cqhead = raw_ring.cq.khead;
        let cqtail = raw_ring.cq.ktail;
        let ring_fd = ring.raw().ring_fd;
        let ring = InnerRing::new(ring, capacity, polled);
        let ring = RefCell::new(ring);
        let ring = Rc::new(ring);
        let unparker_buf = Box::new([0; 8]);
        let unparker_buf = ManuallyDrop::new(unparker_buf);

        Ok(Self {
            ring,
            capacity,
            polled,
            cqhead,
            cqtail,
            ring_fd,
            name,
            allocator,
            unparker: None,
            unparker_buf,
            ring_linked: false,
            preemption_timer: ManuallyDrop::new(None),
        })
    }

    pub(crate) fn unparker(&self) -> Option<&Arc<Unparker>> {
        self.unparker.as_ref()
    }

    pub(crate) fn set_unparker(&mut self, unparker: Arc<Unparker>) {
        self.unparker = Some(unparker)
    }

    pub(crate) fn ring_fd(&self) -> RawFd {
        self.ring_fd
    }

    pub(crate) fn name(&self) -> &'static str {
        &self.name
    }

    pub(crate) fn in_flight(&self) -> u64 {
        self.ring.borrow().in_flight()
    }

    /// Returns true if there are completions availabile for this ring.
    #[inline]
    pub(crate) fn has_completions(&self) -> bool {
        unsafe { *self.cqhead != (*(self.cqtail as *mut AtomicU32)).load(Ordering::Acquire) }
    }

    pub(crate) fn register_buffers(&mut self, buffers: &[IoSlice<'_>]) -> io::Result<()> {
        self.ring
            .borrow_mut()
            .ring
            .registrar()
            .register_buffers(buffers)
    }

    pub(crate) fn unregister_buffers(&mut self) -> io::Result<()> {
        self.ring.borrow_mut().ring.registrar().unregister_buffers()
    }

    /// Install a preemption timer for this ring, which is just a timer that will fire to add some activity to the latency ring.
    ///
    /// Returns true if the preemption timer was installed.
    pub(crate) fn ensure_preempt_timer(&mut self, duration: Duration) -> bool {
        // TODO: Need a way to cancel existing preemption timer.
        if let None = *self.preemption_timer {
            let mut ring = self.ring.borrow_mut();
            if let Some(ref mut sqe) = ring.try_next_sqe() {
                let timeout = duration.into();
                self.preemption_timer.replace(timeout);
                let timeout_ref = &self.preemption_timer.as_ref().unwrap().raw;
                unsafe {
                    sqe.prep_timeout(timeout_ref);
                    sqe.set_user_data(Self::PREEMPT_TIMER_TOKEN);
                }
                true
            } else {
                false
            }
        } else {
            true
        }
    }

    pub(crate) fn turn(&mut self, park_mode: TurnMode) -> io::Result<Turnt> {
        // Fast drain any outstanding CQEs
        let mut skip_park = false;
        let mut progress = Turnt::new();

        // Drain any outstanding CQEs to make room in the SQ.
        progress.completions += self.fast_drain_cqes();

        if self.polled {
            skip_park = true;
        }
        if !self.prepare_park() {
            skip_park = true;
        }
        if progress.completed() > 0 {
            skip_park = true;
        }

        if !skip_park {
            // Satisfy the requested parking mode, submitting SQE's in the process.
            match park_mode {
                TurnMode::Timeout(duration) => {
                    self.block_on_next_cqe_timeout(duration, &mut progress)?;
                }
                TurnMode::NextCompletion => {
                    self.block_on_next_cqe(&mut progress)?;
                }
                TurnMode::EventFDTimeout(eventfd, duration) => {
                    self.block_on_next_eventfd_timeout(eventfd, duration, &mut progress)?;
                }
                TurnMode::EventFD(eventfd) => {
                    self.block_on_next_eventfd(eventfd, &mut progress)?;
                }
                TurnMode::NoPark => {
                    progress.submissions += self.submit_sqes()?;
                    progress.slept = false;
                }
            };
        } else {
            progress.submissions += self.submit_sqes()?;
            progress.slept = false;
        }

        if progress.submitted() > 0 {
            // Drain any outstanding CQEs to make room in the SQ.
            progress.completions += self.fast_drain_cqes();
        }
        // At the end of each loop iteration, we notify any tasks which are waiting for SQ slots, up
        // to the number of SQ slots available.
        self.notify_wakers();
        Ok(progress)
    }

    /// Just drain CQEs, do not attempt to submit.
    pub(crate) fn drain(&mut self) -> io::Result<Turnt> {
        let mut progress = Turnt::new();
        progress.completions += self.fast_drain_cqes();
        Ok(progress)
    }

    /// Prepare the Ring to park. This will first check the cross-thread-wakeup detector to determine
    /// if parking is even an option.
    ///
    /// Returns false if parking should not occur because a cross-thread wakeup was requested, or because
    /// the eventfd poll could not be installed.
    fn prepare_park(&mut self) -> bool {
        if let Some(ref mut unparker) = self.unparker {
            let state = unparker.park();
            if !state.is_parked() {
                let fd = unparker.as_raw_fd();

                let mut ring = self.ring.borrow_mut();
                if let Some(ref mut sqe) = ring.try_next_sqe() {
                    unsafe {
                        sqe.prep_read(fd, &mut self.unparker_buf[..], 8);
                    }
                    sqe.set_user_data(Self::UNPARKER_WAKE_TOKEN);
                } else {
                    return false;
                }
            }
            !state.woken()
        } else {
            true
        }
    }

    fn block_on_next_eventfd(&mut self, eventfd: RawFd, progress: &mut Turnt) -> io::Result<()> {
        let mut ring = self.ring.borrow_mut();
        let skip_park;

        if self.ring_linked {
            skip_park = false;
        } else if let Some(ref mut sqe) = ring.try_next_sqe() {
            unsafe { sqe.prep_poll_add(eventfd, ring_eventfd_pollflags()) };
            sqe.set_user_data(Self::LINKED_RING_TOKEN);
            skip_park = false;
            self.ring_linked = true;
        } else {
            skip_park = true;
        };

        if !skip_park {
            progress.slept = true;
            progress.submissions += ring.submit_and_wait(1)?;
        } else {
            progress.slept = false;
            progress.submissions += ring.submit_sqes()?;
        }
        Ok(())
    }

    fn block_on_next_eventfd_timeout(
        &mut self,
        eventfd: RawFd,
        duration: Duration,
        progress: &mut Turnt,
    ) -> io::Result<()> {
        let mut ring = self.ring.borrow_mut();
        let skip_park;

        if self.ring_linked {
            skip_park = false;
        } else if let Some(ref mut sqe) = ring.try_next_sqe() {
            unsafe { sqe.prep_poll_add(eventfd, ring_eventfd_pollflags()) };
            sqe.set_user_data(Self::LINKED_RING_TOKEN);
            skip_park = false;
            self.ring_linked = true;
        } else {
            skip_park = true;
        };
        if !skip_park {
            if duration == Duration::from_millis(0) {
                progress.slept = false;
                progress.submissions += ring.submit_sqes()?;
            } else {
                progress.slept = true;
                progress.submissions += ring.submit_and_wait_timeout(1, duration)?;
            }
        } else {
            progress.slept = false;
            progress.submissions += ring.submit_sqes()?;
        }
        Ok(())
    }

    fn block_on_next_cqe_timeout(
        &mut self,
        duration: Duration,
        progress: &mut Turnt,
    ) -> io::Result<()> {
        let mut ring = self.ring.borrow_mut();
        if duration == Duration::from_millis(0) {
            progress.slept = false;
            progress.submissions += ring.submit_sqes()?;
        } else {
            progress.slept = true;
            progress.submissions += ring.submit_and_wait_timeout(1, duration)?;
        }
        Ok(())
    }

    fn block_on_next_cqe(&mut self, progress: &mut Turnt) -> io::Result<()> {
        let mut ring = self.ring.borrow_mut();
        progress.slept = true;
        progress.submissions += ring.submit_and_wait(1)?;
        Ok(())
    }

    /// Submit any outstanding SQEs
    pub(crate) fn submit_sqes(&mut self) -> io::Result<usize> {
        let mut ring = self.ring.borrow_mut();
        let submitted = ring.submit_sqes()?;
        Ok(submitted)
    }

    pub(crate) fn notify_wakers(&mut self) {
        let mut ring = self.ring.borrow_mut();
        ring.notify_wakers();
    }

    /// Drain the completion queue for this ring, returning the number of completions drained.
    ///
    /// Draining will proceed in batches of Ring::COMPLETION_BATCH_SIZE. If the unparker token is
    /// encountered, the unparker will be reset signaling that the ring should not park.
    fn fast_drain_cqes(&mut self) -> usize {
        let mut ring = self.ring.borrow_mut();
        ring.drain_cqes(
            &mut self.preemption_timer,
            &mut self.ring_linked,
            &mut self.unparker,
        )
    }

    pub(crate) fn submit_op(&self, op: UnsubmittedOp) -> QueuedOp {
        let ring = self.ring.borrow();
        let notified = ring.submission_control.wait();
        return QueuedOp {
            ring: Rc::clone(&self.ring),
            state: QueuedOpState::Waiting {
                op: Some(op),
                notified,
            },
            poll_count: 0,
        };
    }
}

pin_project! {
    pub(crate) struct QueuedOp {
        ring: Rc<RefCell<InnerRing>>,
        #[pin]
        state: QueuedOpState,
        poll_count: usize,
    }
}

pin_project! {
    #[project = QueuedOpStateProj]
    #[project_replace = QueuedOpStateProjReplace]
    enum QueuedOpState {
        Waiting {
            op: Option<UnsubmittedOp>,
            #[pin] notified: Notified,
        },
        Submitted {
            op: SubmittedOp,
        },
    }
}

impl Future for QueuedOp {
    type Output = <SubmittedOp as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let mut ring = this.ring.borrow_mut();
        *this.poll_count += 1;

        if *this.poll_count > 3 {
            println!("poll count {}", *this.poll_count);
        }
        loop {
            match this.state.as_mut().project() {
                QueuedOpStateProj::Waiting {
                    ref mut op,
                    notified,
                } => {
                    futures_lite::ready!(notified.poll(cx));
                    match ring.next_sqe() {
                        Ok(ref mut sqe) => {
                            let op = op.take().unwrap().attach(sqe);
                            this.state
                                .as_mut()
                                .project_replace(QueuedOpState::Submitted { op });
                        }
                        Err(notify) => {
                            let op = op.take();
                            this.state.as_mut().project_replace(QueuedOpState::Waiting {
                                op,
                                notified: notify,
                            });
                        }
                    }
                }
                QueuedOpStateProj::Submitted { op } => {
                    let result = futures_lite::ready!(Pin::new(op).poll(cx));
                    return Poll::Ready(result);
                }
            };
        }
    }
}

fn ring_eventfd_pollflags() -> iou::PollFlags {
    let flags = iou::PollFlags::POLLERR
        | iou::PollFlags::POLLHUP
        | iou::PollFlags::POLLNVAL
        | iou::PollFlags::POLLIN
        | iou::PollFlags::POLLPRI;
    return flags;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::prelude::FromRawFd;
    use std::time::Instant;

    fn test_ring() -> Ring {
        let allocator = UringBufferAllocator::new(1024 * 10);
        let allocator = Rc::new(allocator);

        let ring = Ring::new(32, "test", allocator, false).unwrap();
        return ring;
    }

    #[test]
    fn test_park_timeout() {
        let mut ring = test_ring();
        let begin_time = Instant::now();
        ring.turn(TurnMode::Timeout(Duration::from_secs(1)))
            .unwrap();
        assert!(Instant::now() - begin_time > Duration::from_secs(1));
    }

    #[test]
    fn test_park_eventfd() {
        let eventfd = crate::sys::create_eventfd().unwrap();
        let mut event = unsafe { std::fs::File::from_raw_fd(eventfd) };

        let mut ring = test_ring();

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(1));
            event.write(&0x1u64.to_ne_bytes()).unwrap();
        });

        ring.turn(TurnMode::EventFD(eventfd)).unwrap();
    }

    #[test]
    fn test_submit_nop() {
        let mut ring = test_ring();
        let unsubmitted = UnsubmittedOp::new(Operation::new_noop());
        let queued = ring.submit_op(unsubmitted);
        futures_lite::pin!(queued);

        loop {
            ring.turn(TurnMode::NoPark).unwrap();
            let mut cx = futures_test::task::noop_context();
            match Pin::new(&mut queued).poll(&mut cx) {
                Poll::Ready(Ok((result, _))) => {
                    assert_eq!(result, 0);
                    break;
                }
                Poll::Ready(Err(_)) => {
                    assert!(false)
                }
                Poll::Pending => continue,
            }
        }
    }

    #[test]
    fn test_cross_thread_wake() {
        let mut ring = test_ring();
        let unparker = Arc::new(Unparker::new().unwrap());
        ring.set_unparker(unparker);
        let unparker = Arc::clone(ring.unparker().unwrap());
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            unparker.wake();
        });
        // Park forever, expecting the eventfd write to wake us up.
        ring.turn(TurnMode::NextCompletion).unwrap();
    }

    #[test]
    fn test_submit_backpressure() {
        let mut ring = test_ring();

        let start_time = std::time::Instant::now();
        let mut to_complete = 1000;
        loop {
            // Submit in batches 2x the ring size to exercise the backpressure mechanism.
            let batch_size = std::cmp::min(ring.capacity * 2, to_complete);
            if batch_size == 0 {
                break;
            }
            let mut in_flight = vec![];
            for _ in 0..batch_size {
                let unsubmitted = UnsubmittedOp::new(Operation::new_noop());
                let queued = ring.submit_op(unsubmitted);
                in_flight.push(Box::pin(queued));
            }

            let mut cx = futures_test::task::noop_context();
            while !in_flight.is_empty() {
                // Try to poll all completions, removing any which have finished.
                'inner: for i in 0..in_flight.len() {
                    let queued = &mut in_flight[i];
                    match Pin::new(queued).poll(&mut cx) {
                        Poll::Ready(Ok((result, _))) => {
                            assert_eq!(result, 0);
                            to_complete -= 1;
                            in_flight.remove(i);
                            break 'inner;
                        }
                        Poll::Ready(_) => {
                            assert!(false)
                        }
                        Poll::Pending => {}
                    }
                }
                ring.turn(TurnMode::NoPark).unwrap();
            }
        }
        let duration = std::time::Instant::now() - start_time;
        println!("took {:?}", duration);
        assert_eq!(0, ring.in_flight())
    }
}
