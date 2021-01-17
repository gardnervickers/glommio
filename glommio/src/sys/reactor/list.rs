use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::Future;
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use intrusive_collections::{container_of, linked_list, offset_of, Adapter, LinkOps, LinkedList};
use intrusive_collections::{LinkedListLink, PointerOps};

struct Inner {
    /// Linked list holding registered listeners.
    list: LinkedList<WaiterAdapter>,
    /// Number of actively registered waiters.
    length: usize,
    /// Number of notified entries
    notified: usize,

    first_unnotified: Option<NonNull<WaiterNode>>,
}

impl Inner {
    fn new() -> Self {
        Inner {
            list: LinkedList::new(WaiterAdapter::new()),
            length: 0,
            notified: 0,
            first_unnotified: None,
        }
    }

    fn insert(&mut self, waiter: Pin<&mut WaiterNode>) {
        assert!(!waiter.link.is_linked());
        let node = unsafe { NonNull::new_unchecked(Pin::into_inner_unchecked(waiter) as *mut _) };
        self.list.push_back(node);
        if self.first_unnotified.is_none() {
            self.first_unnotified = Some(node);
        }
        self.length += 1;
    }

    fn remove(&mut self, waiter: Pin<&mut WaiterNode>) -> WaiterState {
        assert!(waiter.link.is_linked());

        let ptr = unsafe { NonNull::new_unchecked(Pin::get_unchecked_mut(waiter)) };

        let mut cursor = unsafe { self.list.cursor_mut_from_ptr(ptr.as_ptr()) };
        let next = cursor.peek_next().clone_pointer();
        let removed = cursor.remove().expect("entry not found during remove");
        if self.first_unnotified == Some(ptr) {
            self.first_unnotified = next;
        }

        let state = unsafe { removed.as_ref() }
            .state
            .replace(WaiterState::Unlinked);
        if let WaiterState::Notified = state {
            self.notified -= 1;
        }
        self.length -= 1;
        return state;
    }

    fn notify(&mut self, num: usize) {
        if num <= self.notified {
            return;
        }
        let mut cursor = {
            if let Some(first) = self.first_unnotified {
                unsafe { self.list.cursor_mut_from_ptr(first.as_ptr()) }
            } else {
                self.list.front_mut()
            }
        };

        let mut additional = num - self.notified;
        while additional > 0 {
            additional -= 1;
            // Walk the cursor, notifying wakers as we go.
            if let Some(entry) = cursor.get() {
                match entry.state.replace(WaiterState::Notified) {
                    WaiterState::NotPolled => {
                        self.notified += 1;
                    }
                    WaiterState::Polling(waker) => {
                        self.notified += 1;
                        waker.wake();
                    }
                    WaiterState::Unlinked => unreachable!(),
                    WaiterState::Notified => {}
                }
                if additional > 0 {
                    cursor.move_next();
                }
            } else {
                break;
            }
        }

        // Now we know everything up until the current cursor position is notified, so set the new
        // first_unnotified to be the next in the cursor.
        let pointer = cursor.peek_next().clone_pointer();
        self.first_unnotified = pointer;
    }
}

pub(crate) struct Notify {
    inner: Rc<RefCell<Inner>>,
    id: Cell<usize>,
}

impl Notify {
    pub(crate) fn new() -> Notify {
        let inner = Inner::new();
        let inner = RefCell::new(inner);
        let inner = Rc::new(inner);
        Notify {
            inner,
            id: Cell::new(0),
        }
    }

    pub(crate) fn wait(&self) -> Notified {
        let inner = Rc::clone(&self.inner);
        let next_id = self.id.get();
        self.id.set(next_id + 1);
        Notified {
            inner,
            waiter: WaiterNode::new(next_id),
        }
    }

    pub(crate) fn notify(&self, num: usize) {
        self.inner.borrow_mut().notify(num)
    }
}

impl Drop for Notify {
    fn drop(&mut self) {
        let mut inner = self.inner.borrow_mut();
        let num_entries = inner.length;
        inner.notify(num_entries);
    }
}

pub(crate) struct Notified {
    inner: Rc<RefCell<Inner>>,
    waiter: WaiterNode,
}

impl fmt::Debug for Notified {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notified")
            .field("waiter", &self.waiter)
            .finish()
    }
}

impl Future for Notified {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // project to pin and ref.
        let (waiter, inner) = unsafe {
            let this = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut this.waiter), &this.inner)
        };
        let mut list = inner.borrow_mut();
        let state = &waiter.state;
        match state.replace(WaiterState::Notified) {
            WaiterState::NotPolled => {
                state.set(WaiterState::Polling(cx.waker().clone()));
                list.insert(waiter);
                return Poll::Pending;
            }
            WaiterState::Notified => {
                list.remove(waiter);
                return Poll::Ready(());
            }
            WaiterState::Polling(w) => {
                if w.will_wake(cx.waker()) {
                    state.set(WaiterState::Polling(w))
                } else {
                    state.set(WaiterState::Polling(cx.waker().clone()))
                }
                return Poll::Pending;
            }
            WaiterState::Unlinked => panic!("Future cannot be polled after completion"),
        };
    }
}

impl Drop for Notified {
    fn drop(&mut self) {
        let node = unsafe { Pin::new_unchecked(&mut self.waiter) };

        let mut inner = self.inner.borrow_mut();
        let state = inner.remove(node);
        if let WaiterState::Notified = state {
            // Notified, but not polled and removed. We need to move it's notification to another entry.
            inner.notify(1);
        }
    }
}

pub(crate) struct WaiterNode {
    link: LinkedListLink,
    state: Cell<WaiterState>,
    id: usize,
    _p: PhantomPinned,
}

impl WaiterNode {
    fn new(id: usize) -> WaiterNode {
        WaiterNode {
            link: LinkedListLink::new(),
            state: Cell::new(WaiterState::NotPolled),
            _p: PhantomPinned,
            id,
        }
    }

    fn is_unlinked(&self) -> bool {
        self.state.is_unlinked()
    }
}

impl fmt::Debug for WaiterNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.replace(WaiterState::Unlinked);
        let result = f
            .debug_struct("WaiterNode")
            .field("state", &state)
            .field("id", &self.id)
            .finish();
        self.state.replace(state);
        return result;
    }
}

#[derive(Clone)]
enum WaiterState {
    Unlinked,
    NotPolled,
    Notified,
    Polling(Waker),
}

impl WaiterState {
    fn is_unlinked(&self) -> bool {
        if let WaiterState::Unlinked = self {
            true
        } else {
            false
        }
    }
}

impl fmt::Debug for WaiterState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WaiterState::Unlinked => write!(f, "Unlinked"),
            WaiterState::NotPolled => write!(f, "NotPolled"),
            WaiterState::Notified => write!(f, "Notified"),
            WaiterState::Polling(_) => write!(f, "Polling"),
        }
    }
}

struct WaiterPointerOps;

unsafe impl PointerOps for WaiterPointerOps {
    type Value = WaiterNode;
    type Pointer = NonNull<WaiterNode>;

    unsafe fn from_raw(&self, value: *const Self::Value) -> Self::Pointer {
        NonNull::new(value as *mut Self::Value).expect("pointer cannot be null")
    }

    fn into_raw(&self, ptr: Self::Pointer) -> *const Self::Value {
        ptr.as_ptr() as *const Self::Value
    }
}

struct WaiterAdapter {
    pointer_ops: WaiterPointerOps,
    link_ops: linked_list::LinkOps,
}

impl WaiterAdapter {
    fn new() -> WaiterAdapter {
        WaiterAdapter {
            pointer_ops: WaiterPointerOps,
            link_ops: linked_list::LinkOps,
        }
    }
}

unsafe impl Adapter for WaiterAdapter {
    type LinkOps = linked_list::LinkOps;
    type PointerOps = WaiterPointerOps;

    unsafe fn get_value(
        &self,
        link: <Self::LinkOps as LinkOps>::LinkPtr,
    ) -> *const <Self::PointerOps as PointerOps>::Value {
        container_of!(link.as_ptr(), WaiterNode, link)
    }

    unsafe fn get_link(
        &self,
        value: *const <Self::PointerOps as PointerOps>::Value,
    ) -> <Self::LinkOps as intrusive_collections::LinkOps>::LinkPtr {
        if value.is_null() {
            panic!("Passed in pointer to the value can not be null");
        }
        let ptr = (value as *const u8).add(offset_of!(WaiterNode, link));
        //we call unchecked method because of safety check above
        core::ptr::NonNull::new_unchecked(ptr as *mut _)
    }

    fn link_ops(&self) -> &Self::LinkOps {
        &self.link_ops
    }

    fn link_ops_mut(&mut self) -> &mut Self::LinkOps {
        &mut self.link_ops
    }

    fn pointer_ops(&self) -> &Self::PointerOps {
        &self.pointer_ops
    }
}

#[cfg(test)]
mod tests {
    use futures_lite::FutureExt;

    use super::*;

    fn is_notified(notified: &mut Pin<Box<Notified>>) -> bool {
        let mut cx = futures_test::task::noop_context();
        futures_lite::pin!(notified);
        notified.poll(&mut cx).is_ready()
    }

    #[test]
    fn test_notify() {
        let notify = Notify::new();

        let mut l1 = Box::pin(notify.wait());
        let mut l2 = Box::pin(notify.wait());
        let mut l3 = Box::pin(notify.wait());

        assert!(!is_notified(&mut l1));
        assert!(!is_notified(&mut l2));
        assert!(!is_notified(&mut l3));

        notify.notify(3);

        assert!(is_notified(&mut l1));
        assert!(is_notified(&mut l2));
        assert!(is_notified(&mut l3));
    }

    #[test]
    fn test_drop_notified() {
        let notify = Notify::new();

        let mut l1 = Box::pin(notify.wait());
        let mut l2 = Box::pin(notify.wait());
        let mut l3 = Box::pin(notify.wait());

        assert!(!is_notified(&mut l1));
        assert!(!is_notified(&mut l2));
        assert!(!is_notified(&mut l3));

        // If l1 is notified and dropped, only l2 should be notified.
        notify.notify(1);
        drop(l1);
        assert!(is_notified(&mut l2));
        // l2 is polled to completion, so it should not notify l3 on drop.
        drop(l2);
        assert!(!is_notified(&mut l3));
        assert!(!is_notified(&mut l3));
    }

    #[test]
    fn test_wake_count() {
        let (waker, count) = futures_test::task::new_count_waker();
        let mut cx = Context::from_waker(&waker);

        let notify = Notify::new();
        let mut l1 = Box::pin(notify.wait());
        let mut l2 = Box::pin(notify.wait());
        let mut l3 = Box::pin(notify.wait());

        assert!(l1.poll(&mut cx).is_pending());
        assert!(l2.poll(&mut cx).is_pending());
        assert!(l3.poll(&mut cx).is_pending());

        // No wakeups should have been sent.
        assert_eq!(0, count.get());

        // Notify a task, should see a wakeup.
        notify.notify(1);
        assert_eq!(1, count.get());

        // Drop a notified task, the notification should transfer to the next task and we should
        // see another wakeup.
        drop(l1);
        assert_eq!(2, count.get());
        assert!(l2.poll(&mut cx).is_ready());

        notify.notify(10);
        assert_eq!(3, count.get());
        assert!(l3.poll(&mut cx).is_ready());
    }

    #[test]
    fn test_drop_without_notification() {
        let notify = Notify::new();

        let mut l1 = Box::pin(notify.wait());
        let mut l2 = Box::pin(notify.wait());

        assert!(!is_notified(&mut l1));
        assert!(!is_notified(&mut l2));

        drop(l1);
        assert!(!is_notified(&mut l2));
    }
}
