//! Handles for safely interacting with operation state shared with the kernel.
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::sys::op::{Shared, SharedBox};
/// An operation which has not been submitted to the reactor.
///
/// Operations can be submitted to the reactor by calling `Unsubmitted::attach` with a valid SQE.
#[derive(Debug)]
pub(crate) struct Unsubmitted<T>
where
    T: Shared,
{
    inner: Option<SharedBox<T>>,
}

impl<T> Unsubmitted<T>
where
    T: Shared,
{
    pub(crate) fn new(operation: T) -> Self {
        let shared = SharedBox::new(operation);
        Self {
            inner: Some(shared),
        }
    }

    pub(crate) fn attach(mut self, sqe: &mut iou::SubmissionQueueEvent<'_>) -> Submitted<T> {
        let mut inner = self.inner.take().expect("invalid SharedBox state");
        // Safety: SharedBox::attach requires that we only ever call it once per SharedBox, so we enforce
        //         that by moving to a new Submitted type which will not call SharedBox::attach.
        unsafe { inner.attach(sqe) };
        Submitted { inner: Some(inner) }
    }
}

impl<T> Drop for Unsubmitted<T>
where
    T: Shared,
{
    fn drop(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            // Safety: mark_dropped requires that it's only ever called once. This is enforced by taking the
            //         `SharedBox` out of the `Option`.
            unsafe { inner.mark_dropped() }
        }
    }
}

/// An operation which as been submitted to the reactor.
///
/// [`Submitted`] operations share some state with the kernel. Dropping a [`Submitted`] operation
/// will result in the operation being cleaned up upon completion. `Submitted::cancel` can be used
/// to attempt to cancel an operation early.
#[derive(Debug)]
pub(crate) struct Submitted<T>
where
    T: Shared,
{
    inner: Option<SharedBox<T>>,
}

impl<T> Submitted<T>
where
    T: Shared,
{
    /// Cancel this in-flight operation via a cancel operation addded to the provided SQE.
    ///
    /// If this operation is completed, the SQE will be initialized to a NOP.
    pub(crate) fn cancel(mut self, sqe: &mut iou::SubmissionQueueEvent<'_>) -> Cancelled<T> {
        if let Some(ref mut inner) = self.inner {
            // Safety: cancel must only be called once. This is enforced by immediately moving it into `Cancelled`, which
            //         will not call cancel again.
            unsafe {
                inner.cancel(sqe);
            }
        }
        return Cancelled { inner: self };
    }
}

impl<T> Future for Submitted<T>
where
    T: Shared,
{
    type Output = io::Result<(usize, T)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = &mut self.get_mut().inner;
        let handle = inner
            .as_mut()
            .expect("Future should not be polled more than once");

        // Safety: poll_complete requires that once Poll::Ready returns, it's never polled again. This is enforced
        //         by removing the handle from the Option and only replacing it if we see Poll::Pending.
        let result = futures_lite::ready!(unsafe { Pin::new(handle).poll_complete(cx) });
        inner.take();
        return Poll::Ready(result);
    }
}

impl<T> Drop for Submitted<T>
where
    T: Shared,
{
    fn drop(&mut self) {
        if let Some(mut handle) = self.inner.take() {
            unsafe { handle.mark_dropped() }
        }
    }
}

/// An operation which has been submitted to the reactor, but subsequently cancelled. Polling this future
/// will return the result of the original operation once both the original operation and the cancellation
/// have completed.
#[derive(Debug)]
pub(crate) struct Cancelled<T>
where
    T: Shared,
{
    inner: Submitted<T>,
}

impl<T> Future for Cancelled<T>
where
    T: Shared,
{
    type Output = io::Result<(usize, T)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::super::complete_from_cqe;
    use super::*;
    use std::cell::Cell;
    use std::future::Future;
    use std::mem;
    use std::pin::Pin;
    use std::rc::Rc;

    struct DropGuard {
        _sqe: *mut uring_sys::io_uring_sqe,
    }

    impl Drop for DropGuard {
        fn drop(&mut self) {
            unsafe { drop(Box::from_raw(self._sqe)) }
        }
    }

    fn mock_sqe() -> (iou::SubmissionQueueEvent<'static>, impl Drop) {
        let zeroed = unsafe { std::mem::zeroed::<uring_sys::io_uring_sqe>() };
        let leaked = Box::into_raw(Box::new(zeroed));
        // Hack: Take advantage of the layout for single-field structs to emulate a iou::SubmissionQueueEvent.
        struct FakeSQE {
            _sqe: *mut uring_sys::io_uring_sqe,
        }
        let faked = FakeSQE { _sqe: leaked };
        let guard = DropGuard { _sqe: leaked };

        let sqe: iou::SubmissionQueueEvent<'static> = unsafe { mem::transmute(faked) };
        (sqe, guard)
    }

    // Construct some mock SQE's and provide a drop guard to clean them up.

    // Test helper for creating a fake SQE and ensuring it gets dropped.
    fn with_mocks_and_dropchk(
        f: impl FnOnce(
            &mut iou::SubmissionQueueEvent<'_>,
            &mut iou::SubmissionQueueEvent<'_>,
            MockOperation,
        ),
    ) {
        let (mut sqe1, _guard1) = mock_sqe();
        let (mut sqe2, _guard2) = mock_sqe();
        let (operation, dropped) = new_shared_state();
        f(&mut sqe1, &mut sqe2, operation);
        assert!(dropped.get())
    }

    #[derive(Debug)]
    struct MockOperation {
        dropped: Rc<Box<Cell<bool>>>,
    }

    impl Shared for MockOperation {
        unsafe fn prep_sqe(&mut self, _: &mut iou::SubmissionQueueEvent<'_>) {}
    }

    impl Drop for MockOperation {
        fn drop(&mut self) {
            assert!(!self.dropped.get());
            self.dropped.set(true);
        }
    }

    fn new_shared_state() -> (MockOperation, Rc<Box<Cell<bool>>>) {
        let s = Rc::new(Box::new(Cell::new(false)));
        (
            MockOperation {
                dropped: Rc::clone(&s),
            },
            s,
        )
    }

    /// Test that a SubmittedOp returns Poll::Pending when not completed.
    #[test]
    fn test_poll_complete() {
        with_mocks_and_dropchk(|sqe, _, operation| {
            let (waker, awoken_count) = futures_test::task::new_count_waker();
            let mut cx = Context::from_waker(&waker);

            let handle = Unsubmitted::new(operation);
            let mut submission = handle.attach(sqe);

            assert!(Pin::new(&mut submission).poll(&mut cx).is_pending());
            assert_eq!(awoken_count.get(), 0);

            // complete to uphold the contract of attach.
            let cqe = iou::CompletionQueueEvent::from_raw(sqe.raw().user_data, 42, 0);
            unsafe { complete_from_cqe::<MockOperation>(cqe) };
            assert!(Pin::new(&mut submission).poll(&mut cx).is_ready());
        })
    }

    /// Test that a SubmittedOp can be completed, and wakes the waiting task.
    #[test]
    fn test_successful_completion() {
        with_mocks_and_dropchk(|sqe, _, operation| {
            let (waker, awoken_count) = futures_test::task::new_count_waker();

            let handle = Unsubmitted::new(operation);
            let mut submission = handle.attach(sqe);

            let mut cx = Context::from_waker(&waker);
            // build a cqe from the sqe and complete it
            let cqe = iou::CompletionQueueEvent::from_raw(sqe.raw().user_data, 42, 0);

            // Poll once to register the waker
            assert!(Pin::new(&mut submission).poll(&mut cx).is_pending());

            unsafe { complete_from_cqe::<MockOperation>(cqe) };

            assert!(Pin::new(&mut submission).poll(&mut cx).is_ready());
            assert_eq!(awoken_count.get(), 1);
        })
    }

    // Test that an attached operation is properly cleand up if the task drops it's reference to it.
    #[test]
    fn test_dropped_submission() {
        with_mocks_and_dropchk(|sqe, _, operation| {
            let handle = Unsubmitted::new(operation);
            let submission = handle.attach(sqe);
            drop(submission);

            // build a cqe from the sqe and complete it
            let cqe = iou::CompletionQueueEvent::from_raw(sqe.raw().user_data, 42, 0);
            unsafe { complete_from_cqe::<MockOperation>(cqe) };
        })
    }

    // Test that a dropped operation is cleaned up if it's never attached.
    #[test]
    fn test_dropped_before_attach() {
        with_mocks_and_dropchk(|_, _, operation| {
            let handle = Unsubmitted::new(operation);
            drop(handle);
        })
    }

    // Test that a cancelled task can be polled to completion.
    #[test]
    fn test_cancellation_and_polled_completion() {
        with_mocks_and_dropchk(|op_sqe, cancel_sqe, operation| {
            let (waker, awoken_count) = futures_test::task::new_count_waker();
            let mut cx = Context::from_waker(&waker);

            let handle = Unsubmitted::new(operation);
            let attached = handle.attach(op_sqe);
            let mut cancelled = attached.cancel(cancel_sqe);

            // No completions have happened, so future should not be ready
            assert!(Pin::new(&mut cancelled).poll(&mut cx).is_pending());

            // Complete the operation
            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    op_sqe.raw().user_data,
                    0,
                    0,
                ));
            }

            assert_eq!(0, awoken_count.get());

            // Only one completion has happened, so future should not be ready
            assert!(Pin::new(&mut cancelled).poll(&mut cx).is_pending());

            // Complete the cancellation
            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    cancel_sqe.raw().user_data,
                    0,
                    0,
                ));
            }
            assert_eq!(1, awoken_count.get());
            assert!(Pin::new(&mut cancelled).poll(&mut cx).is_ready());
        })
    }

    // Test that a cancelled task which has been dropped is cleaned up after both the original and cancellation SQE's are seen.
    #[test]
    fn test_cancellation_and_drop() {
        with_mocks_and_dropchk(|op_sqe, cancel_sqe, operation| {
            let handle = Unsubmitted::new(operation);
            let attached = handle.attach(op_sqe);
            drop(attached.cancel(cancel_sqe));

            // Completion needs to happen for both SQE's for drop to occur.
            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    op_sqe.raw().user_data,
                    0,
                    0,
                ));
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    cancel_sqe.raw().user_data,
                    0,
                    0,
                ));
            }
        })
    }

    // Test that cancelling a completed op results in a noop sqe.
    #[test]
    fn test_cancelling_completed_op() {
        with_mocks_and_dropchk(|op_sqe, cancel_sqe, operation| {
            let (waker, awoken_count) = futures_test::task::new_count_waker();
            let mut cx = Context::from_waker(&waker);
            let handle = Unsubmitted::new(operation);
            let mut attached = handle.attach(op_sqe);

            assert!(Pin::new(&mut attached).poll(&mut cx).is_pending());

            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    op_sqe.raw().user_data,
                    0,
                    0,
                ));
            }
            let mut cancelled = attached.cancel(cancel_sqe);

            // Since the operation was previously completed, we should be able to poll it without the cancel_sqe.
            // The cancel_sqe should be a noop.
            assert!(Pin::new(&mut cancelled).poll(&mut cx).is_ready());
            assert_eq!(1, awoken_count.get());
            assert_eq!(cancel_sqe.raw().opcode, 0); // 0 for noop
        })
    }

    // Test polling an operation which has been cancelled, but the original completion is not yet there.
    #[test]
    fn test_poll_waiting_cancellation() {
        with_mocks_and_dropchk(|op_sqe, cancel_sqe, operation| {
            let (waker, awoken_count) = futures_test::task::new_count_waker();
            let mut cx = Context::from_waker(&waker);
            let handle = Unsubmitted::new(operation);
            let mut attached = handle.attach(op_sqe);

            assert!(Pin::new(&mut attached).poll(&mut cx).is_pending());

            // cancel the operation
            let mut cancelled = attached.cancel(cancel_sqe);

            // complete the cancellation
            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    cancel_sqe.raw().user_data,
                    0,
                    0,
                ));
            }

            // The operation should still be pending
            assert!(Pin::new(&mut cancelled).poll(&mut cx).is_pending());

            // complete the operation
            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    op_sqe.raw().user_data,
                    42,
                    0,
                ));
            }

            // The operation should finally complete
            assert!(Pin::new(&mut cancelled).poll(&mut cx).is_ready());
            assert_eq!(1, awoken_count.get());
        })
    }

    // Test that an operation dropped waiting on cancellation is properly cleaned up.
    #[test]
    fn test_dropped_waiting_cancel() {
        with_mocks_and_dropchk(|op_sqe, cancel_sqe, operation| {
            let (waker, _) = futures_test::task::new_count_waker();
            let mut cx = Context::from_waker(&waker);
            let handle = Unsubmitted::new(operation);
            let mut attached = handle.attach(op_sqe);

            assert!(Pin::new(&mut attached).poll(&mut cx).is_pending());

            let cancelled = attached.cancel(cancel_sqe);

            // complete the operation
            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    op_sqe.raw().user_data,
                    0,
                    0,
                ));
            }

            // drop the handle
            drop(cancelled);

            // complete the cancellation, allowing for cleanup
            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    cancel_sqe.raw().user_data,
                    0,
                    0,
                ));
            }
        })
    }

    // Test that an operation dropped waiting on completion is properly cleaned up.
    #[test]
    fn test_dropped_waiting_completion() {
        with_mocks_and_dropchk(|op_sqe, cancel_sqe, operation| {
            let (waker, _) = futures_test::task::new_count_waker();
            let mut cx = Context::from_waker(&waker);
            let handle = Unsubmitted::new(operation);
            let mut attached = handle.attach(op_sqe);

            assert!(Pin::new(&mut attached).poll(&mut cx).is_pending());

            let cancelled = attached.cancel(cancel_sqe);

            // complete the cancellation
            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    cancel_sqe.raw().user_data,
                    0,
                    0,
                ));
            }

            // drop the handle
            drop(cancelled);

            // complete the operation, allowing for cleanup
            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    op_sqe.raw().user_data,
                    0,
                    0,
                ));
            }
        })
    }

    // Test that an operation which has already completed can be dropped without polling for completion.
    #[test]
    fn test_dropped_completed() {
        with_mocks_and_dropchk(|op_sqe, _, operation| {
            let (waker, _) = futures_test::task::new_count_waker();
            let mut cx = Context::from_waker(&waker);
            let handle = Unsubmitted::new(operation);
            let mut attached = handle.attach(op_sqe);

            assert!(Pin::new(&mut attached).poll(&mut cx).is_pending());

            // complete the operation
            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    op_sqe.raw().user_data,
                    0,
                    0,
                ));
            }

            drop(attached);
        })
    }

    // Test that polling after completion will panic
    #[test]
    #[should_panic]
    fn test_polled_after_completion() {
        with_mocks_and_dropchk(|op_sqe, _, operation| {
            let (waker, _) = futures_test::task::new_count_waker();
            let mut cx = Context::from_waker(&waker);
            let handle = Unsubmitted::new(operation);
            let mut attached = handle.attach(op_sqe);

            unsafe {
                complete_from_cqe::<MockOperation>(iou::CompletionQueueEvent::from_raw(
                    op_sqe.raw().user_data,
                    0,
                    0,
                ));
            }
            assert!(Pin::new(&mut attached).poll(&mut cx).is_ready());

            let _ = Pin::new(&mut attached).poll(&mut cx);
        })
    }
}
