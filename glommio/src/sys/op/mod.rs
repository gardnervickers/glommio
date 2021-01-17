//! This module provides facilities for safely negotiating shared state between the reactor/kernel and the application.
//!
//! # Example
//!
//! Prepare an `iou::SubmissionQueueEvent` with some shared state, returning a `Future` which will be woken upon completion
//! ```ignore
//! fn submit_noop(waker: Waker, sqe: &mut iou::SubmissionQueueEvent<'_>) -> impl Future<Output = io::Result<(usize, Operation)>> {
//!    let noop = Operation::new_noop();
//!    let shared = Unsubmitted::new(noop);
//!    let submitted = shared.attach(waker, sqe);
//!    return submitted;
//! }
//! ```
//!
//! In concert with the example above, complete an `iou::CompletionQueueEvent`, waking the task associated with the shared state. If
//! no task is currently waiting on the shared state, handle dropping the shared state.
//! ```ignore
//! fn complete_operation(&self) {
//!     let cqe = self.get_next_cqe();
//!     unsafe {
//!         complete_from_cqe::<Operation>(cqe);
//!     }
//! }
//! ```
#![allow(dead_code, unused_imports)]
mod shared;
use shared::SharedBox;
pub(crate) use shared::{complete_from_cqe, Shared};

mod operation;
pub(crate) use operation::Operation;

mod handle;
use handle::{Cancelled, Submitted, Unsubmitted};

pub(crate) type SubmittedOp = Submitted<Operation>;
pub(crate) type UnsubmittedOp = Unsubmitted<Operation>;
pub(crate) type CancelledOp = Cancelled<Operation>;
