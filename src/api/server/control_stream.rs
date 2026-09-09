//! Socket side of a control stream.
//!
//! After `control.open` the connection stays open. A reader thread parses
//! request lines, a writer thread serializes responses, subscription
//! events, and raw terminal records in one order, and the app thread owns
//! attaches through the shared handle.

use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use interprocess::local_socket::traits::Stream as _;
use interprocess::TryClone as _;
use tracing::debug;

use crate::api::control::{ControlConnectionHandle, ControlOutbound};
use crate::api::schema::{
    ControlRecord, EmptyParams, ErrorBody, ErrorResponse, Method, Request, ResponseResult,
    ServerCapabilities, SuccessResponse,
};
use crate::api::subscriptions::ActiveSubscription;
use crate::api::{ApiRequestSender, EventHub};
use crate::ipc::{is_connection_closed_error, LocalStream, LocalStreamReadCount};

use super::pane_graphics_stream::PollBackoff;
use super::{
    dispatch_to_app_with_control, error_response_json, write_text_line, CONNECTION_POLL_INTERVAL,
};

/// Largest request line a control stream accepts; pastes arrive base64 encoded.
const MAX_CONTROL_LINE_BYTES: usize = 8 * 1024 * 1024;
/// How often a blocked reader re-checks that the stream is still alive.
const READER_WAKE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const READ_CHUNK_BYTES: usize = 64 * 1024;

pub(super) fn serve(
    mut stream: LocalStream,
    request_id: String,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    running: &Arc<AtomicBool>,
    capabilities: Option<ServerCapabilities>,
) -> io::Result<()> {
    let (outbound_tx, outbound_rx) = std::sync::mpsc::channel();
    let handle = ControlConnectionHandle::new(outbound_tx);

    let opened = dispatch_to_app_with_control(
        Request {
            id: request_id,
            method: Method::ControlOpen(Default::default()),
        },
        api_tx,
        handle.clone(),
    );
    let open_failed = serde_json::from_str::<serde_json::Value>(&opened)
        .map(|value| value.get("error").is_some())
        .unwrap_or(true);
    // The app registered the connection while answering, so every exit from
    // here on releases it the same way an established stream does.
    if let Err(err) = write_text_line(&mut stream, &opened) {
        close_on_app(&handle, api_tx);
        if is_connection_closed_error(&err) {
            return Ok(());
        }
        return Err(err);
    }
    if open_failed {
        close_on_app(&handle, api_tx);
        return Ok(());
    }

    let writer_stream = match stream.try_clone() {
        Ok(writer_stream) => writer_stream,
        Err(err) => {
            close_on_app(&handle, api_tx);
            return Err(err);
        }
    };
    // The writer must not hold a handle: a handle owns an outbound sender,
    // and the writer only exits once every sender is gone.
    let writer_alive = handle.alive_flag();
    let writer = std::thread::spawn(move || writer_loop(writer_stream, outbound_rx, writer_alive));

    let subscriptions: Arc<Mutex<Vec<ActiveSubscription>>> = Arc::default();
    let poller = {
        let subscriptions = Arc::clone(&subscriptions);
        let handle = handle.clone();
        let api_tx = api_tx.clone();
        let event_hub = event_hub.clone();
        let running = Arc::clone(running);
        std::thread::spawn(move || {
            while handle.is_alive() && running.load(Ordering::Relaxed) {
                if let Ok(mut subscriptions) = subscriptions.lock() {
                    for subscription in subscriptions.iter_mut() {
                        if let Some(event) = subscription.poll(&api_tx, &event_hub) {
                            if !handle.send_line(event.to_string()) {
                                handle.close();
                                break;
                            }
                        }
                    }
                }
                std::thread::sleep(CONNECTION_POLL_INTERVAL);
            }
        })
    };

    let result = reader_loop(
        stream,
        &handle,
        api_tx,
        event_hub,
        running,
        capabilities,
        &subscriptions,
    );

    close_on_app(&handle, api_tx);
    drop(handle);
    let _ = poller.join();
    let _ = writer.join();
    match result {
        Err(err) if is_connection_closed_error(&err) => Ok(()),
        result => result,
    }
}

/// Marks the stream dead and has the app drop its attaches and claims. Safe
/// before `control.open` registered anything: unknown ids are ignored.
fn close_on_app(handle: &ControlConnectionHandle, api_tx: &ApiRequestSender) {
    handle.close();
    let _ = dispatch_to_app_with_control(
        Request {
            id: "control:closed".into(),
            method: Method::ControlClose(EmptyParams::default()),
        },
        api_tx,
        handle.clone(),
    );
}

fn writer_loop(
    mut stream: LocalStream,
    outbound: std::sync::mpsc::Receiver<ControlOutbound>,
    alive: Arc<AtomicBool>,
) {
    while let Ok(item) = outbound.recv() {
        let line = match item {
            ControlOutbound::Line(line) => line,
            ControlOutbound::Snapshot {
                attach_id,
                snapshot,
            } => encode_record(&ControlRecord::Snapshot {
                attach_id,
                snapshot: *snapshot,
            }),
            ControlOutbound::Output {
                attach_id,
                seq,
                bytes,
                budget,
            } => {
                let line = encode_record(&ControlRecord::Output {
                    attach_id,
                    seq,
                    bytes: base64::engine::general_purpose::STANDARD.encode(&bytes),
                });
                budget.release(bytes.len());
                line
            }
            ControlOutbound::Gap {
                attach_id,
                seq,
                dropped_bytes,
            } => encode_record(&ControlRecord::Gap {
                attach_id,
                seq,
                dropped_bytes,
            }),
            ControlOutbound::Detached { attach_id, reason } => {
                encode_record(&ControlRecord::Detached { attach_id, reason })
            }
        };
        if let Err(err) = write_text_line(&mut stream, &line) {
            debug!(err = %err, "control stream write failed");
            alive.store(false, Ordering::Release);
            break;
        }
    }
}

fn encode_record(record: &ControlRecord) -> String {
    serde_json::to_string(record).unwrap_or_else(|_| {
        r#"{"type":"control.error","message":"failed to encode record"}"#.to_string()
    })
}

fn reader_loop(
    mut stream: LocalStream,
    handle: &ControlConnectionHandle,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    running: &Arc<AtomicBool>,
    capabilities: Option<ServerCapabilities>,
    subscriptions: &Mutex<Vec<ActiveSubscription>>,
) -> io::Result<()> {
    // Waking periodically lets the loop notice a dead writer (a client that
    // stopped reading) and release the attaches instead of blocking forever.
    // Named pipes have no receive timeout, so they poll like graphics streams.
    let mut wait = match stream.set_recv_timeout(Some(READER_WAKE_INTERVAL)) {
        Ok(()) => ReaderWait::SocketTimeout,
        Err(err) if err.kind() == io::ErrorKind::Unsupported => {
            crate::ipc::set_local_stream_polling(&mut stream, true)?;
            ReaderWait::Poll(PollBackoff::new())
        }
        Err(err) => return Err(err),
    };
    let mut lines = LineBuffer::default();
    let mut chunk = vec![0u8; READ_CHUNK_BYTES];
    loop {
        while let Some(line) = lines.next_line() {
            if !running.load(Ordering::Relaxed) || !handle.is_alive() {
                return Ok(());
            }
            if handle_request_line(
                line.trim(),
                handle,
                api_tx,
                event_hub,
                &capabilities,
                subscriptions,
            ) == LineOutcome::Close
            {
                return Ok(());
            }
        }
        let room = lines.room();
        if room == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "control stream request line is too large",
            ));
        }
        if !running.load(Ordering::Relaxed) || !handle.is_alive() {
            return Ok(());
        }
        let buf = &mut chunk[..room.min(READ_CHUNK_BYTES)];
        let read = match &mut wait {
            ReaderWait::SocketTimeout => match stream.read(buf) {
                Ok(0) => return Ok(()),
                Ok(read) => read,
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            },
            ReaderWait::Poll(backoff) => {
                match crate::ipc::poll_local_stream_read_count(&mut stream, buf)? {
                    LocalStreamReadCount::Closed => return Ok(()),
                    LocalStreamReadCount::Pending => {
                        std::thread::sleep(backoff.interval);
                        backoff.advance();
                        continue;
                    }
                    LocalStreamReadCount::Data(read) => {
                        backoff.reset();
                        read
                    }
                }
            }
        };
        lines.push(&buf[..read]);
    }
}

enum ReaderWait {
    SocketTimeout,
    Poll(PollBackoff),
}

/// Request bytes split across reads. The line size cap spans every read,
/// so a client cannot grow a line past it by pausing between chunks.
#[derive(Default)]
struct LineBuffer {
    pending: Vec<u8>,
}

impl LineBuffer {
    fn push(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
    }

    /// Bytes the buffer may still take before an unterminated line is too long.
    fn room(&self) -> usize {
        MAX_CONTROL_LINE_BYTES.saturating_sub(self.pending.len())
    }

    fn next_line(&mut self) -> Option<String> {
        let end = self.pending.iter().position(|byte| *byte == b'\n')?;
        let line = self.pending.drain(..=end).collect::<Vec<u8>>();
        Some(String::from_utf8_lossy(&line).into_owned())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum LineOutcome {
    Continue,
    Close,
}

fn handle_request_line(
    line: &str,
    handle: &ControlConnectionHandle,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    capabilities: &Option<ServerCapabilities>,
    subscriptions: &Mutex<Vec<ActiveSubscription>>,
) -> LineOutcome {
    if line.is_empty() {
        return LineOutcome::Continue;
    }

    let request = match serde_json::from_str::<Request>(line) {
        Ok(request) => request,
        Err(err) => {
            let response = ErrorResponse {
                id: request_id_from_line(line),
                error: ErrorBody {
                    code: "invalid_request".into(),
                    message: format!("invalid request: {err}"),
                },
            };
            send_json(handle, &response);
            return LineOutcome::Continue;
        }
    };

    let request_id = request.id.clone();
    let method_name = super::api_method_name(&request.method);
    let changes_ui = crate::api::request_changes_ui(&request);
    crate::logging::api_request_started(&request_id, method_name, changes_ui);
    let response = match request.method {
        Method::ControlOpen(_) => unsupported(request_id.clone(), "control.open"),
        Method::ControlClose(_) => {
            send_json(
                handle,
                &SuccessResponse {
                    id: request_id.clone(),
                    result: ResponseResult::Ok {},
                },
            );
            crate::logging::api_request_completed(&request_id, method_name, "ok", changes_ui);
            return LineOutcome::Close;
        }
        Method::Ping(_) => serde_json::to_string(&SuccessResponse {
            id: request_id.clone(),
            result: ResponseResult::Pong {
                version: crate::build_info::version(),
                protocol: crate::protocol::PROTOCOL_VERSION,
                capabilities: capabilities.clone(),
            },
        })
        .unwrap_or_else(|_| {
            error_response_json(
                request_id.clone(),
                "internal_error",
                "failed to encode response".into(),
            )
        }),
        Method::EventsSubscribe(params) => {
            subscribe(&request_id, params, api_tx, event_hub, subscriptions)
        }
        Method::TerminalInput(params) => terminal_input(handle, request_id.clone(), params),
        Method::EventsWait(_) => unsupported(request_id.clone(), "events.wait"),
        Method::AgentPrompt(_) => unsupported(request_id.clone(), "agent.prompt"),
        Method::AgentWait(_) => unsupported(request_id.clone(), "agent.wait"),
        Method::PaneWaitForOutput(_) => unsupported(request_id.clone(), "pane.wait_for_output"),
        Method::PaneGraphicsStream(_) => unsupported(request_id.clone(), "pane.graphics.stream"),
        method => dispatch_to_app_with_control(
            Request {
                id: request_id.clone(),
                method,
            },
            api_tx,
            handle.clone(),
        ),
    };
    // An empty response means the server already wrote it through the
    // stream so it could order a following record after it.
    let outcome = if response.is_empty() {
        "ok"
    } else {
        super::api_response_outcome(&response)
    };
    crate::logging::api_request_completed(&request_id, method_name, outcome, changes_ui);
    if response.is_empty() {
        return LineOutcome::Continue;
    }
    if handle.send_line(response) {
        LineOutcome::Continue
    } else {
        LineOutcome::Close
    }
}

fn request_id_from_line(line: &str) -> String {
    serde_json::from_str::<serde_json::Value>(line)
        .ok()
        .and_then(|value| value.get("id")?.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn send_json<T: serde::Serialize>(handle: &ControlConnectionHandle, value: &T) -> bool {
    match serde_json::to_string(value) {
        Ok(line) => handle.send_line(line),
        Err(_) => false,
    }
}

fn unsupported(request_id: String, method: &str) -> String {
    error_response_json(
        request_id,
        "unsupported_in_control_stream",
        format!("{method} is not available on a control stream; use a separate connection"),
    )
}

fn subscribe(
    request_id: &str,
    params: crate::api::schema::EventsSubscribeParams,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    subscriptions: &Mutex<Vec<ActiveSubscription>>,
) -> String {
    let event_start_sequence = event_hub.current_sequence();
    let mut added = Vec::with_capacity(params.subscriptions.len());
    for (index, subscription) in params.subscriptions.into_iter().enumerate() {
        match ActiveSubscription::new(
            subscription,
            request_id,
            index,
            api_tx,
            event_hub,
            event_start_sequence,
        ) {
            Ok(active) => added.push(active),
            Err(response) => {
                return serde_json::to_string(&response).unwrap_or_else(|_| {
                    error_response_json(
                        request_id.to_owned(),
                        "invalid_subscription",
                        "invalid subscription".into(),
                    )
                });
            }
        }
    }
    if let Ok(mut subscriptions) = subscriptions.lock() {
        subscriptions.extend(added);
    }
    serde_json::to_string(&SuccessResponse {
        id: request_id.to_owned(),
        result: ResponseResult::SubscriptionStarted {},
    })
    .unwrap_or_else(|_| "{}".to_string())
}

fn terminal_input(
    handle: &ControlConnectionHandle,
    request_id: String,
    params: crate::api::schema::TerminalInputParams,
) -> String {
    let bytes = match base64::engine::general_purpose::STANDARD.decode(&params.bytes) {
        Ok(bytes) => bytes,
        Err(err) => {
            return error_response_json(
                request_id,
                "invalid_request",
                format!("invalid terminal.input bytes: {err}"),
            );
        }
    };
    let Some(sink) = handle.input_sink(&params.attach_id) else {
        return error_response_json(
            request_id,
            "unknown_attach",
            format!("attach {} is not live", params.attach_id),
        );
    };
    match sink.try_send(bytes::Bytes::from(bytes)) {
        Ok(()) => serde_json::to_string(&SuccessResponse {
            id: request_id,
            result: ResponseResult::Ok {},
        })
        .unwrap_or_else(|_| "{}".to_string()),
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => error_response_json(
            request_id,
            "input_full",
            "terminal input queue is full; retry".into(),
        ),
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => error_response_json(
            request_id,
            "terminal_closed",
            "terminal no longer accepts input".into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_buffer_reassembles_a_line_split_across_reads() {
        let mut lines = LineBuffer::default();
        lines.push(b"{\"id\":\"1\",");
        assert!(lines.next_line().is_none());
        lines.push(b"\"method\":\"ping\"}\n{\"id\":\"2\"");
        assert_eq!(
            lines.next_line().as_deref(),
            Some("{\"id\":\"1\",\"method\":\"ping\"}\n")
        );
        assert!(lines.next_line().is_none());
        lines.push(b"}\n");
        assert_eq!(lines.next_line().as_deref(), Some("{\"id\":\"2\"}\n"));
    }

    /// The cap counts every read of an unterminated line, so pausing between
    /// chunks buys no extra room.
    #[test]
    fn line_buffer_room_shrinks_across_reads() {
        let mut lines = LineBuffer::default();
        assert_eq!(lines.room(), MAX_CONTROL_LINE_BYTES);
        lines.push(&vec![b'a'; MAX_CONTROL_LINE_BYTES - 1]);
        assert_eq!(lines.room(), 1);
        lines.push(b"a");
        assert_eq!(lines.room(), 0);
        assert!(lines.next_line().is_none());
    }
}
