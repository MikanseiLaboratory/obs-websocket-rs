//! Callback registration.
//!
//! Callbacks run on the driver task. They must not wait on [`crate::Client::request`];
//! that waits for the same task and will not complete.

use std::sync::{Arc, Mutex, Weak};

use obs_websocket_core::Event;

use crate::client::ConnectionState;

type EventCallback = dyn Fn(&Event) + Send + Sync;
type StateCallback = dyn Fn(&ConnectionState) + Send + Sync;

struct Registration<T: ?Sized> {
    id: u64,
    callback: Arc<T>,
}

pub(crate) struct Registry {
    next_id: u64,
    events: Vec<Registration<EventCallback>>,
    states: Vec<Registration<StateCallback>>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            next_id: 1,
            events: Vec::new(),
            states: Vec::new(),
        }
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    pub(crate) fn push_event(&mut self, callback: Arc<EventCallback>) -> u64 {
        let id = self.alloc_id();
        self.events.push(Registration { id, callback });
        id
    }

    pub(crate) fn push_state(&mut self, callback: Arc<StateCallback>) -> u64 {
        let id = self.alloc_id();
        self.states.push(Registration { id, callback });
        id
    }

    pub(crate) fn remove(&mut self, id: u64) {
        self.events.retain(|registration| registration.id != id);
        self.states.retain(|registration| registration.id != id);
    }

    pub(crate) fn event_callbacks(&self) -> Vec<Arc<EventCallback>> {
        self.events
            .iter()
            .map(|registration| Arc::clone(&registration.callback))
            .collect()
    }

    pub(crate) fn state_callbacks(&self) -> Vec<Arc<StateCallback>> {
        self.states
            .iter()
            .map(|registration| Arc::clone(&registration.callback))
            .collect()
    }
}

/// Unregisters a callback when dropped.
pub struct Subscription {
    id: u64,
    registry: Weak<Mutex<Registry>>,
}

impl Subscription {
    pub(crate) fn new(id: u64, registry: &Arc<Mutex<Registry>>) -> Self {
        Self {
            id,
            registry: Arc::downgrade(registry),
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            if let Ok(mut registry) = registry.lock() {
                registry.remove(self.id);
            }
        }
    }
}
