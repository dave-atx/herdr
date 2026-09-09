//! Shared state between a control stream's socket threads and the server.
//!
//! The socket side owns the reader and writer threads; the server side owns
//! terminal attaches and geometry claims. They meet through this handle.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::api::schema::{TerminalDetachReason, TerminalSnapshot};
use crate::pane::raw_stream::{RawInputSink, RawTapBudget};

/// Version of the control stream contract advertised in `ping` capabilities.
pub const CONTROL_STREAM_PROTOCOL: u32 = 1;

/// One line or record queued for a control stream's writer thread.
#[derive(Debug)]
pub(crate) enum ControlOutbound {
    /// A finished JSON line (response or subscription event).
    Line(String),
    Snapshot {
        attach_id: String,
        snapshot: Box<TerminalSnapshot>,
    },
    Output {
        attach_id: String,
        seq: u64,
        bytes: Bytes,
        budget: Arc<RawTapBudget>,
    },
    Gap {
        attach_id: String,
        seq: u64,
        dropped_bytes: u64,
    },
    Detached {
        attach_id: String,
        reason: TerminalDetachReason,
    },
}

pub(crate) type ControlOutboundSender = std::sync::mpsc::Sender<ControlOutbound>;

pub(crate) struct ControlConnectionShared {
    /// Server-assigned id; zero until `control.open` is handled.
    id: AtomicU64,
    /// Shared with threads that must not keep the outbound channel open.
    alive: Arc<AtomicBool>,
    outbound: ControlOutboundSender,
    /// Direct PTY input sinks per attach id, filled by the server on attach so
    /// keystrokes never wait behind the app loop.
    inputs: Mutex<HashMap<String, RawInputSink>>,
}

#[derive(Clone)]
pub struct ControlConnectionHandle(Arc<ControlConnectionShared>);

impl ControlConnectionHandle {
    pub(crate) fn new(outbound: ControlOutboundSender) -> Self {
        Self(Arc::new(ControlConnectionShared {
            id: AtomicU64::new(0),
            alive: Arc::new(AtomicBool::new(true)),
            outbound,
            inputs: Mutex::new(HashMap::new()),
        }))
    }

    pub(crate) fn id(&self) -> u64 {
        self.0.id.load(Ordering::Acquire)
    }

    pub(crate) fn assign_id(&self, id: u64) {
        self.0.id.store(id, Ordering::Release);
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.0.alive.load(Ordering::Acquire)
    }

    /// Liveness flag without the outbound sender, for the writer thread.
    pub(crate) fn alive_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.0.alive)
    }

    pub(crate) fn close(&self) {
        self.0.alive.store(false, Ordering::Release);
    }

    pub(crate) fn outbound(&self) -> ControlOutboundSender {
        self.0.outbound.clone()
    }

    pub(crate) fn send_line(&self, line: String) -> bool {
        self.0.outbound.send(ControlOutbound::Line(line)).is_ok()
    }

    pub(crate) fn register_input(&self, attach_id: String, sink: RawInputSink) {
        if let Ok(mut inputs) = self.0.inputs.lock() {
            inputs.insert(attach_id, sink);
        }
    }

    pub(crate) fn unregister_input(&self, attach_id: &str) {
        if let Ok(mut inputs) = self.0.inputs.lock() {
            inputs.remove(attach_id);
        }
    }

    pub(crate) fn input_sink(&self, attach_id: &str) -> Option<RawInputSink> {
        self.0.inputs.lock().ok()?.get(attach_id).cloned()
    }
}
