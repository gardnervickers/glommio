#![allow(dead_code)]

use log::warn;

use crate::sys::reactor::unparker::Unparker;
use crate::sys::reactor::Ring;
use crate::sys::uring::UringBufferAllocator;
use std::io::IoSlice;
use std::ops::AddAssign;

use std::rc::Rc;

use std::sync::Arc;

use std::io;
use std::time::Duration;

use super::ring::{TurnMode, Turnt};

/// [`Rings`] is a container for the 3 rings each Glommio thread uses for IO.
pub(crate) struct Rings {
    main_ring: Ring,
    latency_ring: Ring,
    poll_ring: Ring,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TurnStats {
    submitted: usize,
    completed: usize,
    slept: bool,
}

impl TurnStats {
    pub(crate) fn new() -> TurnStats {
        TurnStats {
            submitted: 0,
            completed: 0,
            slept: false,
        }
    }

    pub(crate) fn submitted(&self) -> usize {
        self.submitted
    }

    pub(crate) fn completed(&self) -> usize {
        self.completed
    }

    pub(crate) fn slept(&self) -> bool {
        self.slept
    }
}

impl AddAssign<Turnt> for TurnStats {
    fn add_assign(&mut self, rhs: Turnt) {
        self.submitted += rhs.submitted();
        self.completed += rhs.completed();
        if rhs.slept() {
            self.slept = true
        }
    }
}

pub(crate) enum ParkMode {
    /// Park the ring until the first event, or the provided duration elapses.
    Timeout(Duration, Option<Duration>),
    /// Park the ring until the first event.
    Wait,
    /// Park and immediately unpark the ring.
    NoPark,
}

impl Rings {
    pub(crate) fn new(
        allocator: Rc<UringBufferAllocator>,
        registry: &[IoSlice<'_>],
    ) -> io::Result<Self> {
        let mut main_ring = Ring::new(128, "main", Rc::clone(&allocator), false)?;
        let latency_ring = Ring::new(128, "latency", Rc::clone(&allocator), false)?;
        let mut poll_ring = Ring::new(128, "poll", Rc::clone(&allocator), true)?;
        let unparker = Arc::new(Unparker::new()?);
        main_ring.set_unparker(unparker);

        match main_ring.register_buffers(&registry) {
            Err(x) => warn!(
                "Error: registering buffers in the main ring. Skipping{:#?}",
                x
            ),
            Ok(_) => match poll_ring.register_buffers(&registry) {
                Err(x) => {
                    warn!(
                        "Error: registering buffers in the poll ring. Skipping{:#?}",
                        x
                    );
                    main_ring.unregister_buffers().unwrap();
                }
                Ok(_) => {
                    allocator.activate_registered_buffers(0);
                }
            },
        }

        Ok(Self {
            main_ring,
            latency_ring,
            poll_ring,
        })
    }

    pub(crate) fn needs_preempt(&self) -> bool {
        self.latency_ring.has_completions()
    }

    pub(crate) fn unparker(&self) -> Arc<Unparker> {
        Arc::clone(self.main_ring.unparker().unwrap())
    }

    pub(crate) fn main_ring_mut(&mut self) -> &mut Ring {
        &mut self.main_ring
    }

    pub(crate) fn latency_ring_mut(&mut self) -> &mut Ring {
        &mut self.latency_ring
    }

    pub(crate) fn poll_ring_mut(&mut self) -> &mut Ring {
        &mut self.poll_ring
    }

    /// Drive io submission and completion on the rings.
    ///
    /// The provided `user_timer` and `preempt_timer` are used to signal the parking mode for this set of rings.
    /// The `user_timer` corresponds to a user timeout and is used as an upper-bound for how long to sleep the ring. The
    /// `preempt_timer` is a bit different, and exists to interrupt repeated polling of the rings in order to move to a new
    /// task queue.
    ///
    /// Essentially, the `preempt_timer` guarantees that at some point we'll end up with some completions in the latency ring
    /// which will trigger preemption in the reactor.
    pub(crate) fn park(&mut self, mode: ParkMode) -> io::Result<TurnStats> {
        let mut turn_stats = TurnStats::new();

        // First, drain everything to see if we have new events.
        turn_stats += self.main_ring.drain()?;
        turn_stats += self.latency_ring.drain()?;
        turn_stats += self.poll_ring.drain()?;

        // Decide if we should sleep. If there were no events from the previous round of drains, and the poll ring does not have anything
        // in-flight, then we can sleep.
        let mut skip_sleep = turn_stats.completed > 0 || self.latency_ring.in_flight() > 0;

        match mode {
            ParkMode::Timeout(timeout, preempt_timer) => {
                if let Some(preempt) = preempt_timer {
                    if !self.latency_ring.ensure_preempt_timer(preempt) {
                        skip_sleep = true;
                    }
                }
                if !skip_sleep {
                    let latency_ring_fd = self.latency_ring.ring_fd();
                    turn_stats += self
                        .main_ring
                        .turn(TurnMode::EventFDTimeout(latency_ring_fd, timeout))?;
                } else {
                    turn_stats += self.main_ring.turn(TurnMode::NoPark)?;
                    turn_stats += self.latency_ring.turn(TurnMode::NoPark)?;
                    turn_stats += self.poll_ring.turn(TurnMode::NoPark)?;
                }
            }
            ParkMode::Wait => {
                if !skip_sleep {
                    let latency_ring_fd = self.latency_ring.ring_fd();
                    turn_stats += self.main_ring.turn(TurnMode::EventFD(latency_ring_fd))?;
                } else {
                    turn_stats += self.main_ring.turn(TurnMode::NoPark)?;
                    turn_stats += self.latency_ring.turn(TurnMode::NoPark)?;
                    turn_stats += self.poll_ring.turn(TurnMode::NoPark)?;
                }
            }
            ParkMode::NoPark => {
                // Turn all rings without sleeping
                turn_stats += self.main_ring.turn(TurnMode::NoPark)?;
                turn_stats += self.latency_ring.turn(TurnMode::NoPark)?;
                turn_stats += self.poll_ring.turn(TurnMode::NoPark)?;
            }
        }

        Ok(turn_stats)
    }
}
