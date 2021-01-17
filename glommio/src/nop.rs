//! A simple nop task for the reactor.

use std::io;
use std::rc::{Rc, Weak};

use crate::parking::Reactor;
use crate::Local;

pub struct NopSubmitter {
    reactor: Weak<Reactor>,
}

impl NopSubmitter {
    pub fn new() -> Self {
        let reactor = Local::get_reactor();
        let reactor = Rc::downgrade(&reactor);
        Self { reactor }
    }

    pub async fn run_nop(&self) -> io::Result<()> {
        let reactor = self.reactor.upgrade().unwrap();
        reactor.noop().await?;
        Ok(())
    }
}
