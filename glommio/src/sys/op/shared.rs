//! Facilities for issuing io_uring requests to the reactor, managing shared state and reaping completions.
//!
//! The approach taken here is (adapted from Ringbahn)[https://github.com/ringbahn/ringbahn/blob/master/src/ring/completion.rs] with changes
//! to make it suitable for Glommio's execution model.
//!
//! ### Safety
//! In general, this module should be considered unsafe, and is not intended to be exposed outside of the parent module.
//!
//! ### Design
//! The core of this module is the [`SharedBox<T>`] construct, which is a reference to shared memory (`RawShared`) needed to configure and run an operation, and
//! a state machine for tracking the status and ownership of the operation.
//!
//! [`SharedBox<T>`] can logically be thought of as a two "halves" of the same heap allocation.
//! ```ascii
//! +-------------+-------------+
//! |             |             |
//! | Application |   Kernel    |
//! |             |             |
//! +-------------+-------------+
//!
//! The application side support attaching the [`SharedBox<T>`] to a CQE, cancelling the operation, marking the internal [`RawShared`] as dropped, and
//! polling for completion while the kernel side supports delivering completion/cancellation events.
//! The state machine inside of [`RawShared`] negotiates these actions between the application and the kernel side.
//!
//! The central idea is to go from `Empty` -> `Submitted` -> `Completed` -> `Empty` via cooperation by the kernel and the application, however
//! cancellation and the fact that the application can `Drop` it's reference to the [`RawShared`] at any time requires that we have additional
//! states to track this. As a result, the state machine is a more complicated, looking something like this.
//!
//!
//! +-------------------+
//! |     Empty         |
//! +-------------------+
//!         |
//!         |
//!         v
//! +-------------------+      +-------------------+
//! |     Submitted     |----->|  SubmittedCancel  |----------------+
//! +-------------------+      +-------------------+                |
//!          |                           |                          |
//!          |                           |                          |
//!          v                           v                          v
//! +-------------------+      +-------------------+      +-------------------+
//! |     Completed     |      | PendingCompletion |<---->|  PendingCancel    |
//! +-------------------+      +-------------------+      +-------------------+
//!          |                           |                          |
//!          |                           |                          |
//!          v                           v                          |
//! +-------------------+      +-------------------+                |
//! |     Empty         |<---- |     Completed     |<---------------+
//! +-------------------+      +-------------------+
//!
//! ### Drop Rules
//!
//! At each point along this state machine transition, we need to acknowledge that the application may drop it's reference to the
//! [`RawShared`]. When this happens, we need to determine when the shared state can be safely dropped. This boils down to the following
//! rules for handling `Drop`.
//!
//! 1. `Empty` indicates there are no in-flight operations for the shared state, so the application is responsible or dropping the [`SharedBox<T>`].
//! 2. If `Submitted`, a [`RawShared`] can only be dropped once the CQE for the operation completes.
//! 3. If `SubmittedCancel` a [`RawShared`] can only be dropped once the CQE for the operation and the CQE for the cancellation completes.
//!    This is because we have no guarantees as to ordering between the original operation CQE and the cancellation CQE. In a pathological
//!    scenario, the original operation could complete and it's pointer be reused while the cancellation has not yet been submitted. This would
//!    at best result in unintended cancellation and at worst, UB due to derefencing invalid memory.
//! 4. If `Completed`, the [`RawShared`] must be dropped by the application to avoid leaking memory.
//!
//! ### Completion Rules
//!
//! Completion is fairly straightforward without cancellation, a transition to `Completed` is only made once the CQE for the operation completes.
//! However with cancellation, completion is dependent on both the original CQE finishing and the CQE for the io_uring completion op finishing. There
//! are no ordering guarantees between the two CQE's, so we need to support transitioning between `PendingCompletion` and `PendingCancel` until we've
//! seen both before moving to `Completed`.
//!
//! The tricky part of this is that we don't know if a CQE is for cancellation, or for completion. CQE's don't carry that information. To make a distinction
//! between the two, we use a tagged pointer for the `user_data` field of the SQE, which is carried over to the CQE. The `user_data` field is opaque to io_uring
//! by design, so a valid pointer is not necessary. We can take advantage of this by using a pointer tagging scheme where we use the low bits to differentiate between
//! a regular operation, and cancellation. Alignment to at least 4 bytes and as a power of 2 is verified statically for `NonNull<UnsafeCell<RawShared<()>>>`. This means
//! we'll always have a low bit which we can use for tagging.
//!
//! Upon CQE completion, we check the low bit for the user_data u64 to determine if it is a cancellation or a completion. This informs the next state change for the [`RawShared<T>`],
//! allowing us to enforce the following invariants:
//!
//! 5. If completing the cancellation CQE in `SubmittedCancel`, we need to wait for a corresponding completion of the original CQE before dropping or completing.
//! 6. If completing the cancellation CQE in `PendingCancel`, this means that the original CQE was already completed and a transition to `Completed` is required, making the
//!    result availabile to the task.
//! 7. If completing the original CQE in `Submitted` or `PendingCompletion`, we need to transition to `Completed`, making the result available to the task.
//! 8. If completing the original CQE in `SubmittedCancel`, we need to wait for the corresponding cancellation event by transitioning to `PendingCancel`.
//! 9. When transitioning to `Completed`, we must check the `task_dropped` flag to determine if the polling task still has a reference to the shared state.
//!    If the polling task has dropped it's reference, it is the responsiblity of the completion function to cleanup the [`RawShared`].
//!
//! ### Other thoughts
//!
//! In following these rules, we can safely negotiate both completion conditions and drop behavior between the kernel and submitting tasks, assuming external cooperation
//! from the parent module. Most of the safety invariants necessary are around calling methods only once, so the parent module supports this by transfering ownership from
//! `Unsubmitted` -> `Submitted` -> `Cancelled`.
//!
//! One downside with the current approach is that it does not support linking SQE's internally. This would need to be handled externally at a higher level.
//!
//! Reuse of the allocation for each [`RawShared`] is a desirable feature. While this is not currently supported, we could return the allocation in the complete_* functions
//! back to the reactor for pooling in some cases, like cancellation.
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::process::abort;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};
use std::{alloc, fmt, io, mem};

use mem::align_of;

use crate::sys::op::{Cancelled, Submitted};

/// [`Shared`] implementations can construct [`iou::SubmissionQueueEvent`]'s, optionally referencing shared state owned by the implementor.
pub(crate) trait Shared: 'static {
    /// Prepare an SQE for submission to the reactor.
    ///
    /// ### Safety
    /// After returning, the provided [`iou::SubmissionQueueEvent`] must be valid. Any references to shared state used when preparing the SQE must
    /// be owned by the implementer.
    unsafe fn prep_sqe(&mut self, sqe: &mut iou::SubmissionQueueEvent<'_>);
}

/// Core state machine for the `RawShared` struct. The state machine tracks the following progression:
#[derive(Debug)]
enum Phase {
    /// The SQE associated with this operation has been submitted to the kernel.
    Submitted(Option<Waker>),
    SubmittedCancel(Option<Waker>),
    PendingCancel(Option<Waker>, io::Result<usize>),
    PendingCompletion(Option<Waker>),
    Completed(io::Result<usize>),
    Empty,
}

impl Default for Phase {
    fn default() -> Self {
        Phase::Empty
    }
}

#[derive(Debug)]
struct State {
    /// True if the submitting task has dropped it's reference to to the SharedBox.
    task_dropped: bool,
    phase: Phase,
}

impl State {
    fn new() -> State {
        Self {
            task_dropped: false,
            phase: Phase::Empty,
        }
    }
    fn attachable(&self) -> bool {
        match self.phase {
            Phase::Empty => true && !self.task_dropped,
            _ => false,
        }
    }
}

/// [`RawShared`] holds shared state between the application and the reactor.
#[derive(Debug)]
struct RawShared<T> {
    operation: T,
    state: State,
}

/// [`SharedBox<T>`] allows waiting, completing, cancelling, and dropping state associated with an in-flight IO operation.
/// These rules allow for safely transitioning ownership between the task and the reactor depending on the state of the submitted operation.
pub(super) struct SharedBox<T>
where
    T: Shared,
{
    inner: Option<NonNull<UnsafeCell<RawShared<T>>>>,
}

impl<T> fmt::Debug for SharedBox<T>
where
    T: Shared,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SharedBox")
    }
}

impl<T> SharedBox<T>
where
    T: Shared,
{
    /// Wrap the provided [`Operation`] in a [`RawShared`] state machine.
    ///
    /// This will leak if dropped. It is intended to be wrapped by either a [`Unsubmitted`] or [`Submitted`] handle.
    pub(super) fn new(operation: T) -> Self {
        // TODO: We need to support allocation pooling to make this efficient. This means we need to take an allocator as a parameter,
        //       probably backed by a slab or something. We can then initialize a new value from that.
        let state = State::new();
        let shared = RawShared { operation, state };
        let shared = UnsafeCell::new(shared);

        let ptr = Box::leak(Box::new(shared));
        let inner = Some(ptr.into());
        SharedBox { inner }
    }

    /// Attach the operation captured by this [`SharedBox`] to the provided [`iou::SubmissionQueueEvent`].
    ///
    /// ### Safety
    /// `SharedBox::attach` should only ever be called once for the lifetime of the `SharedBox`.
    pub(super) unsafe fn attach(&mut self, sqe: &mut iou::SubmissionQueueEvent<'_>) {
        let shared = &mut *self.inner.unwrap().as_mut().get();
        assert!(shared.state.attachable());

        #[cfg(not(miri))]
        shared.operation.prep_sqe(sqe);

        shared.state.phase = Phase::Submitted(None);
        drop(shared);
        let addr = UserdataPointer::<T>::tag_completion(self.inner.unwrap());
        sqe.set_user_data(addr as u64);
    }

    /// Mark this [`SharedBox`] as cancelled, adding the cancellation event to the provided iou::SubmissionQueueEvent.
    ///
    /// ### Safety:
    /// `SharedBox::cancel` should only ever be called once for the lifetime of the `SharedBox`.
    pub(super) unsafe fn cancel(&mut self, sqe: &mut iou::SubmissionQueueEvent<'_>) {
        // Safety: We have a unique reference to self.
        let state = &mut (&mut *self.inner.unwrap().as_mut().get()).state;

        match mem::take(&mut state.phase) {
            Phase::Submitted(w) => {
                // If submitted, change the state to cancelled and prep the sqe.
                state.phase = Phase::SubmittedCancel(w);
                drop(state);
                let addr = UserdataPointer::<T>::tag_cancellation(self.inner.unwrap());
                #[cfg(not(miri))]
                sqe.prep_cancel(addr as u64);
                sqe.set_user_data(addr as u64);
            }
            Phase::Completed(r) => {
                // If the operation has already completed, there is nothing to do but we still need a valid SQE so prep a nop.
                state.phase = Phase::Completed(r);
                #[cfg(not(miri))]
                sqe.prep_nop();
                sqe.set_user_data(0);
            }

            Phase::Empty
            | Phase::SubmittedCancel(_)
            | Phase::PendingCancel(_, _)
            | Phase::PendingCompletion(_) => {
                unreachable!()
            }
        }
    }

    /// Poll for completion of the [`Operation`] submitted to the Kernel.
    ///
    /// ### Safety:
    /// - After this returns Poll::Ready(_), this method should never be called again.
    pub(super) unsafe fn poll_complete(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(usize, T)>> {
        // Safety: We have a unique reference to self.
        let state = &mut (&mut *self.inner.unwrap().as_mut().get()).state;
        let task_dropped = state.task_dropped;
        debug_assert!(!task_dropped, "poll called on a dropped SharedBox ref");

        match mem::take(&mut state.phase) {
            Phase::Submitted(w) => {
                state.phase = Phase::Submitted(replace_waker(w, cx));
            }
            Phase::SubmittedCancel(w) => {
                state.phase = Phase::SubmittedCancel(replace_waker(w, cx));
            }
            Phase::PendingCancel(w, r) => {
                state.phase = Phase::PendingCancel(replace_waker(w, cx), r);
            }
            Phase::PendingCompletion(w) => {
                state.phase = Phase::PendingCompletion(replace_waker(w, cx));
            }
            Phase::Completed(result) => {
                drop(state); // Drop mutable reference to state so we can drop the whole thing.

                // Safety: We drop the outstanding mutable ref state and get a new ptr to reconstruct the box.
                let shared = Box::from_raw(self.inner.take().unwrap().as_ptr());
                let shared = shared.into_inner();
                let operation_data = shared.operation;
                match result {
                    Ok(ret) => return Poll::Ready(Ok((ret, operation_data))),
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
            Phase::Empty => unreachable!(),
        }
        return Poll::Pending;
    }

    /// Mark this [`SharedBox`] as dropped.
    ///
    /// ### Safety
    /// This method should only ever be called once for the lifetime of the SharedBox.
    pub(super) unsafe fn mark_dropped(&mut self) {
        // Safety: We have a unique reference to self.
        if let Some(mut v) = self.inner.take() {
            let state = &mut (*v.as_mut().get()).state;
            debug_assert!(!state.task_dropped, "SharedBox dropped more than once");
            state.task_dropped = true;
            match mem::take(&mut state.phase) {
                // Prop 4: If the operation is still in-flight, we cannot drop the operation state.
                //         Mark it as droppeed instead to signal to the reactor that it should clean up the shared operation state.
                Phase::Submitted(w) => {
                    state.phase = Phase::Submitted(w);
                }

                Phase::SubmittedCancel(w) => {
                    state.phase = Phase::SubmittedCancel(w);
                }

                // Prop 6: If the shared state has been completed or is empty, we need to clean up the state ourselves.
                Phase::Completed(_) | Phase::Empty => {
                    // Safety: The reactor has either completed the request, or the request was never submitted
                    //         so we are the sole owners. It's safe to drop.
                    drop(state);
                    let shared = Box::from_raw(v.as_ptr());
                    drop(shared);
                }

                // We are waiting on a completion still, so we cannot drop the internal state. Set the drop flag and we'll handle it during CQE completion.
                Phase::PendingCancel(waker, result) => {
                    state.phase = Phase::PendingCancel(waker, result);
                }

                Phase::PendingCompletion(waker) => {
                    state.phase = Phase::PendingCompletion(waker);
                }
            }
        }
    }
}

/// [`CompletionHandle<T>`] allows completing and dropping state associated with an in-flight IO operation. It is intended to be used from the reactor
/// side, the complement to [`SharedBox<T>`]
pub(super) struct CompletionHandle<T>
where
    T: Shared,
{
    inner: NonNull<UnsafeCell<RawShared<T>>>,
}

impl<T> CompletionHandle<T>
where
    T: Shared,
{
    /// Notify the SharedBox that the cancellation CQE has been seen for this operation.
    ///
    /// ### Safety
    /// This method should only ever be called once, and relies on SharedBox::cancel only ever being called once.
    unsafe fn complete_cancellation(
        mut self,
        _: io::Result<usize>,
    ) -> Option<Box<UnsafeCell<RawShared<T>>>> {
        // Safety: We have ownership of self.
        let state = &mut (&mut *self.inner.as_mut().get()).state;
        let task_dropped = state.task_dropped;
        match mem::take(&mut state.phase) {
            Phase::SubmittedCancel(w) => {
                // We completed cancellation successfully, but have not yet seen a CQE for the original request.
                // TODO: We ignore the cancellation result in favor of the result from the original operation. It would be relatively easy
                //       to capture both in the future though if we have a good way to display these to the user.
                state.phase = Phase::PendingCompletion(w);
                return None;
            }

            Phase::PendingCancel(w, result) => {
                // We were waiting on a cancellation, and that was just satisfied.
                // Complete the request if the task was not dropped, otherwise drop the state ourselves.
                if task_dropped {
                    // The state has been dropped and is not waiting on cancellation. We can cleanup directly.
                    drop(state);
                    // Safety: There are no mutable references to the inner state, so we can reconstruct the box.
                    let shared = Box::from_raw(self.inner.as_ptr());
                    return Some(shared);
                } else {
                    state.phase = Phase::Completed(result);
                    if let Some(waker) = w {
                        waker.wake();
                    }
                    return None;
                }
            }

            // - `Phase::Submitted` should never have an outstanding cancellation CQE.
            // - `Phase::PendingCompletion` should never occur more than once, and is set by this method.
            // - `Phase::Completed` should only happen after all in-flight CQE's have been reaped.
            Phase::Empty
            | Phase::Submitted(_)
            | Phase::PendingCompletion(_)
            | Phase::Completed(_) => unreachable!(),
        };
    }

    /// Notify the SharedBox that the original operation CQE has been seen for this operation.
    ///
    /// ### Safety
    /// This method should only ever be called once, and relies on SharedBox::attach only ever being called once.
    unsafe fn complete(
        mut self,
        result: io::Result<usize>,
    ) -> Option<Box<UnsafeCell<RawShared<T>>>> {
        // Safety: We have ownership of self.
        let state = &mut (&mut *self.inner.as_mut().get()).state;
        let task_dropped = state.task_dropped;
        match mem::take(&mut state.phase) {
            // Prop 3: The state transitions to State::Completed when the reactor completes the associated request.
            Phase::Submitted(waker) => {
                if task_dropped {
                    // The state has been dropped and is not waiting on cancellation. We can cleanup directly.
                    drop(state);
                    // Safety: There are no mutable references to the inner state, so we can reconstruct the box.
                    let shared = Box::from_raw(self.inner.as_ptr());
                    return Some(shared);
                } else {
                    state.phase = Phase::Completed(result);
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                    return None;
                }
            }

            Phase::SubmittedCancel(w) => {
                // We completed the request successfully, but have not yet seen a CQE for the cancellation.
                state.phase = Phase::PendingCancel(w, result);
                return None;
            }

            Phase::PendingCompletion(w) => {
                // We were waiting on completion of the original request, and that was just satisfied.
                // Complete the request if the task was not dropped, otherwise drop the state ourselves.
                if task_dropped {
                    // The state has been dropped and is not waiting on cancellation. We can cleanup directly.
                    drop(state);
                    // Safety: There are no mutable references to the inner state, so we can reconstruct the box.
                    let shared = Box::from_raw(self.inner.as_ptr());
                    return Some(shared);
                } else {
                    state.phase = Phase::Completed(result);
                    if let Some(waker) = w {
                        waker.wake();
                    }
                    return None;
                }
            }

            // - `Phase::Submitted` should never have an outstanding cancellation CQE.
            // - `Phase::PendingCancel` should never occur more than once, and is set by this method.
            // - `Phase::Completed` should only happen after all in-flight CQE's have been reaped.
            Phase::Empty | Phase::PendingCancel(_, _) | Phase::Completed(_) => {
                unreachable!()
            }
        }
    }
}

/// Complete the operation associated with the provided cqe.
///
/// TODO: It would be nice if complete_from_cqe returned sometehing so that we could reuse the RawShared allocation if it was in State::Dropped. This would be useful for pooling submissions
///       in the reactor.
///
/// Safety: The provided CQE must correspond to a `iou::SubmissionQueueEvent` which has been prepared with `SharedBox::attach`. Additionally,
///         the type parameter `T` must match the type parameter used to prepare to `iou::SubmissionQueueEvent` which resulted in this `iou::CompletionQueueEvent`.
pub(crate) unsafe fn complete_from_cqe<T>(cqe: iou::CompletionQueueEvent) -> bool
where
    T: Shared,
{
    let result = cqe.result();
    let user_data = cqe.user_data();
    debug_assert_ne!(user_data, uring_sys::LIBURING_UDATA_TIMEOUT);
    debug_assert!(user_data <= std::usize::MAX as u64);
    let ptr: UserdataPointer<T> = UserdataPointer::from_raw(user_data as usize);
    match ptr {
        UserdataPointer::Null => false,
        UserdataPointer::Cancellation(shared) => {
            let handle = CompletionHandle { inner: shared };
            handle.complete_cancellation(result);
            true
        }
        UserdataPointer::Completion(shared) => {
            let handle = CompletionHandle { inner: shared };
            handle.complete(result);
            true
        }
    }
}

// Replace a waker if needed
#[inline]
fn replace_waker(old: Option<Waker>, cx: &mut Context<'_>) -> Option<Waker> {
    old.map(|old| {
        if old.will_wake(cx.waker()) {
            old
        } else {
            cx.waker().clone()
        }
    })
    .or_else(|| Some(cx.waker().clone()))
}

#[inline]
fn replace_waker_ref(old: &mut Option<Waker>, cx: &mut Context<'_>) {
    if let Some(ref old_waker) = old {
        if !old_waker.will_wake(cx.waker()) {
            *old = Some(cx.waker().clone())
        }
    } else {
        *old = Some(cx.waker().clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UserdataPointer<T> {
    Null,
    Cancellation(NonNull<UnsafeCell<RawShared<T>>>),
    Completion(NonNull<UnsafeCell<RawShared<T>>>),
}

impl<T> UserdataPointer<T> {
    const CANCELLATION_TAG: usize = 0x01;
    const ALIGNMENT: usize = mem::align_of::<NonNull<UnsafeCell<RawShared<T>>>>();
    const TAG_MASK: usize = 0x03;
    const PTR_MASK: usize = !0x03;

    #[inline]
    fn tag_completion(ptr: NonNull<UnsafeCell<RawShared<T>>>) -> usize {
        let ptr = ptr.as_ptr() as usize;
        ptr
    }

    #[inline]
    fn tag_cancellation(ptr: NonNull<UnsafeCell<RawShared<T>>>) -> usize {
        let ptr = ptr.as_ptr() as usize;
        (ptr & Self::PTR_MASK) | (Self::CANCELLATION_TAG & Self::TAG_MASK)
    }

    #[inline]
    fn check_cancellation(ptr: usize) -> (bool, usize) {
        (
            ptr & Self::TAG_MASK == Self::CANCELLATION_TAG,
            ptr & Self::PTR_MASK,
        )
    }

    #[inline]
    fn from_raw(ptr: usize) -> Self {
        let (is_cancellation, real_ptr) = Self::check_cancellation(ptr);
        debug_assert_eq!(real_ptr & Self::TAG_MASK, 0, "unaligned");
        let ptr = real_ptr as *mut UnsafeCell<RawShared<T>>;
        if let Some(shared) = NonNull::new(ptr) {
            if is_cancellation {
                UserdataPointer::Cancellation(shared)
            } else {
                UserdataPointer::Completion(shared)
            }
        } else {
            UserdataPointer::Null
        }
    }
}

// Ensure that alignment is a power of 2
const ALIGNED_TO: &() = &[()][1
    - ((mem::align_of::<RawShared<()>>() & (mem::align_of::<RawShared<super::Operation>>() - 1))
        == 0) as usize];
// Ensure that alignment is larger than 4
const _: &() = &[()][1 - (mem::align_of::<RawShared<super::Operation>>() >= 4) as usize];

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;

    #[test]
    fn test_ptr_tagging() {
        let ptr: NonNull<UnsafeCell<RawShared<()>>> = NonNull::dangling();
        let ptr_raw = ptr.as_ptr() as usize;
        let completion = UserdataPointer::<()>::tag_completion(ptr);
        let cancellation = UserdataPointer::<()>::tag_cancellation(ptr);
        let null_ptr = std::ptr::null_mut() as *mut NonNull<UnsafeCell<RawShared<()>>> as usize;

        assert_eq!(
            UserdataPointer::<()>::check_cancellation(completion),
            (false, ptr_raw)
        );
        assert_eq!(
            UserdataPointer::<()>::check_cancellation(cancellation),
            (true, ptr_raw)
        );
        assert_eq!(
            UserdataPointer::<()>::check_cancellation(null_ptr),
            (false, null_ptr)
        );
    }
}
