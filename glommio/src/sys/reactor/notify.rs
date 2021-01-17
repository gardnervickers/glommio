//! [`Notify`] supports condvar-like wait/notify for async tasks.
//! The implementation here is inspired by the (https://github.com/smol-rs/event-listener)[event-listener] crate. The main
//! differences being that this implementation does not use atomics/locks and there is no support for notifying additional
//! tasks prior to registration.
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use intrusive_collections::{intrusive_adapter, UnsafeRef};
use intrusive_collections::{LinkedList, LinkedListLink};
use std::marker::PhantomPinned;

pub(crate) struct Notify {
    inner: Rc<RefCell<Inner>>,
}

/// Notify allows for registering tasks to wait for some condition.
impl Notify {
    /// Construct a new Notify instance.
    pub(crate) fn new() -> Notify {
        let inner = Inner::new();
        let inner = RefCell::new(inner);
        let inner = Rc::new(inner);
        Notify { inner }
    }

    /// Wait on a condition. Returns a future which can be used to wait.
    pub(crate) fn wait(&self) -> Notified {
        let entry = self.inner.borrow_mut().new_entry();
        let inner = Rc::clone(&self.inner);
        Notified {
            inner,
            entry: Some(entry),
        }
    }

    /// Notify up to num tasks, waking them up.
    pub(crate) fn notify(&self, num: usize) {
        self.inner.borrow_mut().notify_entries(num)
    }
}

impl Drop for Notify {
    fn drop(&mut self) {
        // Notify everything on drop
        let mut inner = self.inner.borrow_mut();
        let num_entries = inner.length;
        inner.notify_entries(num_entries);
    }
}

/// [`Notification`] is a Future which can be waited on until notified by a [`Notify`] instance.
pub(crate) struct Notified {
    /// Reference to the inner state of the Notify
    inner: Rc<RefCell<Inner>>,
    /// Pointer to this notifications entry in the linked list.
    entry: Option<UnsafeRef<Waiter>>,
}

impl Future for Notified {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let entry = self
            .entry
            .take()
            .expect("cannot poll completed Notification");
        let mut list = self.inner.borrow_mut();
        let state = &entry.as_ref().state;
        match state.replace(WaiterState::Notified) {
            WaiterState::NotPolled => {
                state.set(WaiterState::Polling(cx.waker().clone()));
                drop(list);
                self.entry.replace(entry);
                return Poll::Pending;
            }
            WaiterState::Notified => {
                list.remove_entry(entry);
                drop(list);
                return Poll::Ready(());
            }
            WaiterState::Polling(w) => {
                if w.will_wake(cx.waker()) {
                    state.set(WaiterState::Polling(w))
                } else {
                    state.set(WaiterState::Polling(cx.waker().clone()))
                }
                drop(list);
                self.entry.replace(entry);
                return Poll::Pending;
            }
        }
    }
}

impl Drop for Notified {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            let mut inner = self.inner.borrow_mut();
            if let WaiterState::Notified = inner.remove_entry(entry) {
                // notify an additional entry if this entry was notified, but the notfication was never picked up by the task.
                inner.notify_entries(1);
            }
        }
    }
}

struct Waiter {
    link: LinkedListLink,
    state: Cell<WaiterState>,
    _p: PhantomPinned,
}

impl Waiter {
    fn new() -> Waiter {
        Waiter {
            link: LinkedListLink::new(),
            state: Cell::new(WaiterState::NotPolled),
            _p: PhantomPinned,
        }
    }
}

enum WaiterState {
    NotPolled,
    Notified,
    Polling(Waker),
}

intrusive_adapter!(EntryAdapter = UnsafeRef<Waiter>: Waiter { link: LinkedListLink });

/// Inner state for the notifier
struct Inner {
    /// Linked list holding registered listeners.
    list: LinkedList<EntryAdapter>,
    /// Number of actively registered waiters.
    length: usize,
    /// Number of notified entries currently in the list.
    notified: usize,
}

impl Inner {
    fn new() -> Inner {
        Inner {
            list: LinkedList::new(EntryAdapter::NEW),
            length: 0,
            notified: 0,
        }
    }

    fn new_entry(&mut self) -> UnsafeRef<Waiter> {
        // construct a new entry for the list on the heap.
        let entry = Box::new(Waiter::new());
        let entry = UnsafeRef::from_box(entry);
        self.list.push_back(UnsafeRef::clone(&entry));
        self.length += 1;
        return entry;
    }

    /// Remove an entry from the entry list. This will decrement the count of notified entries and drop the entry.
    fn remove_entry(&mut self, entry: UnsafeRef<Waiter>) -> WaiterState {
        let mut cursor = unsafe { self.list.cursor_mut_from_ptr(entry.as_ref()) };
        let removed = cursor.remove();
        let removed = unsafe { removed.map(|v| UnsafeRef::into_box(v)) }.expect("entry not found");
        let state = removed.state.replace(WaiterState::NotPolled);
        if let WaiterState::Notified = state {
            self.notified -= 1;
        }
        self.length -= 1;
        return state;
    }

    /// Notify up to `num` entries.
    fn notify_entries(&mut self, num: usize) {
        if num <= self.notified {
            return;
        }
        let mut additional = num - self.notified;

        let mut cursor = self.list.front_mut();
        while additional > 0 {
            if let Some(entry) = cursor.get() {
                match entry.state.replace(WaiterState::Notified) {
                    WaiterState::NotPolled => {
                        additional -= 1;
                        self.notified += 1;
                    }
                    WaiterState::Notified => {}
                    WaiterState::Polling(waker) => {
                        additional -= 1;
                        self.notified += 1;
                        waker.wake();
                    }
                }
                cursor.move_next();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_poll_pending(mut f: impl Future + Unpin) {
        let mut cx = futures_test::task::noop_context();
        match Pin::new(&mut f).poll(&mut cx) {
            Poll::Ready(_) => assert!(false, "expected future to be pending"),
            Poll::Pending => {}
        }
    }

    fn assert_poll_ready(mut f: impl Future + Unpin) {
        let mut cx = futures_test::task::noop_context();
        match Pin::new(&mut f).poll(&mut cx) {
            Poll::Ready(_) => {}
            Poll::Pending => assert!(false, "expected future to be ready"),
        }
    }

    fn assert_one_ready<F: Future + Unpin>(mut f1: F, mut f2: F) -> F {
        let f1_ready;
        let f2_ready;

        let mut cx = futures_test::task::noop_context();
        match Pin::new(&mut f1).poll(&mut cx) {
            Poll::Ready(_) => {
                f1_ready = true;
            }
            Poll::Pending => f1_ready = false,
        };
        match Pin::new(&mut f2).poll(&mut cx) {
            Poll::Ready(_) => {
                f2_ready = true;
            }
            Poll::Pending => f2_ready = false,
        };
        assert!(
            !(f1_ready && f2_ready),
            "expected only one future to be ready"
        );
        if f1_ready {
            return f2;
        } else {
            return f1;
        }
    }

    #[test]
    fn test_poll_notification() {
        let notify = Notify::new();
        let mut waiter = notify.wait();

        assert_poll_pending(&mut waiter);
        notify.notify(1);
        assert_poll_ready(&mut waiter);
    }

    #[test]
    fn test_poll_notifications() {
        let notify = Notify::new();
        let mut waiter1 = notify.wait();
        let mut waiter2 = notify.wait();

        assert_poll_pending(&mut waiter1);
        assert_poll_pending(&mut waiter2);

        notify.notify(1);
        let mut other = assert_one_ready(waiter1, waiter2);

        notify.notify(1);
        assert_poll_ready(&mut other);
    }

    #[test]
    #[should_panic(expected = "cannot poll completed Notification")]
    fn test_panic_double_poll() {
        let notify = Notify::new();
        let mut waiter = notify.wait();

        assert_poll_pending(&mut waiter);
        notify.notify(1);
        assert_poll_ready(&mut waiter);
        assert_poll_ready(&mut waiter);
    }

    #[test]
    fn test_notify_next_on_notification_drop() {
        let notify = Notify::new();
        let waiter1 = notify.wait();
        let mut waiter2 = notify.wait();

        notify.notify(1);
        drop(waiter1);
        assert_poll_ready(&mut waiter2);
    }

    #[test]
    fn test_notify_all_on_drop() {
        let notify = Notify::new();
        let mut waiter1 = notify.wait();
        let mut waiter2 = notify.wait();
        drop(notify);
        assert_poll_ready(&mut waiter1);
        assert_poll_ready(&mut waiter2);
    }

    #[test]
    fn test_dropping_polled_notified_does_not_wake() {
        let notify = Notify::new();
        let mut waiter1 = notify.wait();
        let mut waiter2 = notify.wait();

        notify.notify(1);
        assert_poll_ready(&mut waiter1);
        drop(waiter1);
        assert_poll_pending(&mut waiter2)
    }
}
