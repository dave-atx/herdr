use super::*;

use crate::api::control::{ControlConnectionHandle, ControlOutbound};
use crate::api::schema::{
    Method, Request, TabChrome, TabSetGeometryParams, TerminalAttachGeometry, TerminalAttachMode,
    TerminalAttachParams, TerminalAttachTarget, TerminalDetachReason, TerminalQueryAuthority,
};

/// Sends a request as a control stream would and returns the response the
/// app handed back, or `None` when the server wrote it through the stream.
fn send_control(
    server: &mut HeadlessServer,
    handle: &ControlConnectionHandle,
    method: Method,
) -> Option<serde_json::Value> {
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request: Request {
            id: "control-test".into(),
            method,
        },
        respond_to,
        response_write_complete: None,
        stream_active: None,
        control: Some(handle.clone()),
    });
    let response = response_rx.recv().expect("control response");
    if response.is_empty() {
        return None;
    }
    Some(serde_json::from_str(&response).expect("json"))
}

fn next_line(outbound: &std::sync::mpsc::Receiver<ControlOutbound>) -> serde_json::Value {
    match outbound.try_recv() {
        Ok(ControlOutbound::Line(line)) => serde_json::from_str(&line).expect("json line"),
        other => panic!("expected a response line, got {other:?}"),
    }
}

fn focused_runtime(server: &HeadlessServer) -> &crate::terminal::TerminalRuntime {
    let pane_id = server.app.state.workspaces[0]
        .focused_pane_id()
        .expect("focused pane");
    server
        .app
        .state
        .runtime_for_pane_in_workspace(&server.app.terminal_runtimes, 0, pane_id)
        .expect("test runtime")
}

fn attach_params(target: &str) -> TerminalAttachParams {
    TerminalAttachParams {
        target: target.to_owned(),
        mode: TerminalAttachMode::Raw,
        history_limit_bytes: None,
        answer_queries: TerminalQueryAuthority::Client,
        takeover: false,
        geometry: TerminalAttachGeometry::Tab,
        cols: None,
        rows: None,
        cell_width_px: 0,
        cell_height_px: 0,
    }
}

fn attach(
    server: &mut HeadlessServer,
    handle: &ControlConnectionHandle,
    outbound: &std::sync::mpsc::Receiver<ControlOutbound>,
    params: TerminalAttachParams,
) -> serde_json::Value {
    let response = send_control(server, handle, Method::TerminalAttach(params));
    assert!(
        response.is_none(),
        "a successful attach answers through the stream, got {response:?}"
    );
    next_line(outbound)
}

#[tokio::test]
async fn control_stream_attaches_streams_and_releases_on_close() {
    let mut server = test_headless_server();
    let mut input_rx = install_focused_test_runtime(&mut server, b"hello\r\n");
    let snapshot = server.app.session_snapshot();
    let pane_id = snapshot.focused_pane_id.clone().expect("focused pane");
    let tab_id = snapshot.focused_tab_id.clone().expect("focused tab");
    let (outbound_tx, outbound_rx) = std::sync::mpsc::channel();
    let handle = ControlConnectionHandle::new(outbound_tx);

    let opened = send_control(
        &mut server,
        &handle,
        Method::ControlOpen(Default::default()),
    )
    .expect("open response");
    assert_eq!(opened["result"]["type"], "control_opened");
    let connection_id = opened["result"]["connection_id"]
        .as_u64()
        .expect("connection id");
    assert_eq!(handle.id(), connection_id);
    assert!(server.control_connections.contains_key(&connection_id));

    let attached = attach(&mut server, &handle, &outbound_rx, attach_params(&pane_id));
    assert_eq!(
        attached["result"]["type"], "terminal_attached",
        "attach failed: {attached}"
    );
    let attach_id = attached["result"]["attach_id"]
        .as_str()
        .expect("attach id")
        .to_owned();
    let terminal_id = attached["result"]["terminal_id"]
        .as_str()
        .expect("terminal id")
        .to_owned();
    assert_eq!(attached["result"]["pane_id"], pane_id);
    assert_eq!(
        server.terminal_attach_owners.get(&terminal_id),
        Some(&connection_id)
    );

    match outbound_rx.try_recv() {
        Ok(ControlOutbound::Snapshot {
            attach_id: snapshot_attach,
            snapshot,
        }) => {
            assert_eq!(snapshot_attach, attach_id);
            assert_eq!(snapshot.seq, 0);
            assert!(snapshot
                .primary
                .as_deref()
                .is_some_and(|primary| primary.contains("hello")));
            assert_eq!(snapshot.state.cols, 80);
            assert_eq!(snapshot.state.rows, 24);
        }
        other => panic!("expected the attach snapshot after the response, got {other:?}"),
    }

    let runtime = focused_runtime(&server);
    runtime.test_process_pty_bytes(b"world");
    match outbound_rx.try_recv() {
        Ok(ControlOutbound::Output { seq, bytes, .. }) => {
            assert_eq!(seq, 1);
            assert_eq!(&bytes[..], b"world");
        }
        other => panic!("expected raw output after the snapshot, got {other:?}"),
    }

    handle
        .input_sink(&attach_id)
        .expect("input sink")
        .try_send(bytes::Bytes::from_static(b"x"))
        .expect("input accepted");
    assert_eq!(
        input_rx.recv().await.expect("pty input"),
        bytes::Bytes::from_static(b"x")
    );

    let resized = send_control(
        &mut server,
        &handle,
        Method::TabSetGeometry(TabSetGeometryParams {
            tab_id: tab_id.clone(),
            cols: 120,
            rows: 40,
            cell_width_px: 8,
            cell_height_px: 16,
            chrome: TabChrome::None,
        }),
    )
    .expect("geometry response");
    assert!(
        server.app.state.control_chromeless_tabs.contains(&tab_id),
        "chrome: none marks the tab chromeless"
    );
    assert_eq!(
        resized["result"]["type"], "ok",
        "geometry failed: {resized}"
    );
    assert_eq!(
        server.tab_geometry_controllers.get(&tab_id),
        Some(&connection_id)
    );
    let runtime = focused_runtime(&server);
    assert_ne!(
        runtime.current_size(),
        (24, 80),
        "tab geometry resized the pane"
    );
    let layout = match outbound_rx.try_recv() {
        Ok(ControlOutbound::Line(line)) => {
            serde_json::from_str::<serde_json::Value>(&line).expect("layout json")
        }
        other => panic!("expected a tab.layout record before any resize output, got {other:?}"),
    };
    assert_eq!(layout["type"], "tab.layout");
    assert_eq!(layout["layout"]["area"]["width"], 120);
    assert_eq!(layout["layout"]["area"]["height"], 40);
    let (rows, cols) = runtime.current_size();
    assert_eq!(layout["layout"]["panes"][0]["rect"]["width"], cols);
    assert_eq!(layout["layout"]["panes"][0]["rect"]["height"], rows);
    // Chromeless: no scrollbar gutter or border shaved off the pane.
    assert_eq!((rows, cols), (40, 120));

    let snapshot_again = send_control(
        &mut server,
        &handle,
        Method::TerminalSnapshot(TerminalAttachTarget {
            attach_id: attach_id.clone(),
        }),
    )
    .expect("snapshot response");
    assert_eq!(snapshot_again["result"]["type"], "ok");
    let records = outbound_rx.try_iter().collect::<Vec<_>>();
    assert!(
        records.iter().any(|record| matches!(
            record,
            ControlOutbound::Snapshot { snapshot, .. } if snapshot.seq == 1
        )),
        "re-snapshot carries the current sequence"
    );

    let closed = send_control(
        &mut server,
        &handle,
        Method::ControlClose(Default::default()),
    )
    .expect("close response");
    assert_eq!(closed["result"]["type"], "ok");
    assert!(server.control_connections.is_empty());
    assert!(server.terminal_attach_owners.is_empty());
    assert!(server.tab_geometry_controllers.is_empty());
    assert!(!handle.is_alive());
    let records = outbound_rx.try_iter().collect::<Vec<_>>();
    assert!(
        records.iter().any(|record| matches!(
            record,
            ControlOutbound::Detached {
                reason: TerminalDetachReason::Closed,
                ..
            }
        )),
        "closing the stream detaches every attach"
    );
}

#[tokio::test]
async fn second_control_attach_needs_takeover_and_evicts_the_first() {
    let mut server = test_headless_server();
    let _input_rx = install_focused_test_runtime(&mut server, b"");
    let pane_id = server
        .app
        .session_snapshot()
        .focused_pane_id
        .expect("focused pane");
    let (first_tx, first_rx) = std::sync::mpsc::channel();
    let first = ControlConnectionHandle::new(first_tx);
    let (second_tx, second_rx) = std::sync::mpsc::channel();
    let second = ControlConnectionHandle::new(second_tx);
    send_control(&mut server, &first, Method::ControlOpen(Default::default()));
    send_control(
        &mut server,
        &second,
        Method::ControlOpen(Default::default()),
    );

    let attached = attach(&mut server, &first, &first_rx, attach_params(&pane_id));
    assert_eq!(attached["result"]["type"], "terminal_attached");
    let first_attach = attached["result"]["attach_id"]
        .as_str()
        .expect("attach id")
        .to_owned();

    let refused = send_control(
        &mut server,
        &second,
        Method::TerminalAttach(attach_params(&pane_id)),
    )
    .expect("refusal response");
    assert_eq!(refused["error"]["code"], "terminal_attached");

    let mut takeover = attach_params(&pane_id);
    takeover.takeover = true;
    let taken = attach(&mut server, &second, &second_rx, takeover);
    assert_eq!(taken["result"]["type"], "terminal_attached");
    assert!(first.input_sink(&first_attach).is_none());
    let records = first_rx.try_iter().collect::<Vec<_>>();
    assert!(records.iter().any(|record| matches!(
        record,
        ControlOutbound::Detached {
            reason: TerminalDetachReason::Takeover,
            ..
        }
    )));
}

#[tokio::test]
async fn control_methods_need_an_open_stream() {
    let mut server = test_headless_server();
    let _input_rx = install_focused_test_runtime(&mut server, b"");
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request: Request {
            id: "plain".into(),
            method: Method::TerminalAttach(attach_params("w1:p1")),
        },
        respond_to,
        response_write_complete: None,
        stream_active: None,
        control: None,
    });
    let response: serde_json::Value =
        serde_json::from_str(&response_rx.recv().expect("response")).expect("json");
    assert_eq!(response["error"]["code"], "control_stream_required");
}
