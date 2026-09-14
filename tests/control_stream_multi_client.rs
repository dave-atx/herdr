//! Two control streams sharing one pane: attaches coexist, tab geometry
//! follows the last stream to claim, type, or survive, and `control.list`
//! reports both.

#![cfg(unix)]

pub mod support;

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use serde_json::Value;
use support::{
    cleanup_test_base, register_runtime_dir, register_spawned_herdr_pid,
    unregister_spawned_herdr_pid,
};

fn unique_test_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    PathBuf::from(format!(
        "/tmp/herdr-control-multi-{}-{nanos}",
        std::process::id()
    ))
}

struct SpawnedHerdr {
    _master: Box<dyn MasterPty + Send>,
    child: Box<dyn Child + Send + Sync>,
}

impl Drop for SpawnedHerdr {
    fn drop(&mut self) {
        let pid = self.child.process_id();
        let _ = self.child.kill();
        if let Some(pid) = pid {
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline {
                let mut status = 0;
                let done = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, libc::WNOHANG) };
                if done == pid as libc::pid_t || done == -1 {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
            unregister_spawned_herdr_pid(Some(pid));
        }
    }
}

fn wait_for_socket(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() && UnixStream::connect(path).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("socket did not appear at {}", path.display());
}

fn spawn_server(config: &Path, runtime: &Path, api: &Path) -> SpawnedHerdr {
    fs::create_dir_all(config.join("herdr")).unwrap();
    fs::create_dir_all(runtime).unwrap();
    register_runtime_dir(runtime);
    fs::write(config.join("herdr/config.toml"), "onboarding = false\n").unwrap();
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let mut cmd = CommandBuilder::new(env!("CARGO_BIN_EXE_herdr"));
    cmd.arg("server");
    cmd.env("XDG_CONFIG_HOME", config);
    cmd.env("XDG_RUNTIME_DIR", runtime);
    cmd.env("HERDR_SOCKET_PATH", api);
    cmd.env_remove("HERDR_CLIENT_SOCKET_PATH");
    cmd.env("SHELL", "/bin/sh");
    cmd.env_remove("HERDR_ENV");
    let child = pair.slave.spawn_command(cmd).unwrap();
    register_spawned_herdr_pid(child.process_id());
    drop(pair.slave);
    SpawnedHerdr {
        _master: pair.master,
        child,
    }
}

fn api_request(socket: &Path, request: &str) -> Value {
    let mut stream = UnixStream::connect(socket).unwrap();
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).unwrap();
    serde_json::from_str(&response).unwrap()
}

/// Creates a workspace and returns its root pane id and tab id.
fn create_pane(socket: &Path, label: &str) -> (String, String) {
    let result = api_request(
        socket,
        &format!(r#"{{"id":"create","method":"workspace.create","params":{{"label":"{label}"}}}}"#),
    );
    assert!(
        result.get("error").is_none(),
        "workspace.create failed: {result}"
    );
    let pane = result
        .pointer("/result/root_pane/pane_id")
        .unwrap()
        .as_str()
        .unwrap()
        .into();
    let tab = result
        .pointer("/result/tab/tab_id")
        .unwrap()
        .as_str()
        .unwrap()
        .into();
    (pane, tab)
}

fn pane_input(socket: &Path, pane: &str, text: &str) {
    let escaped = text.replace('"', "\\\"");
    let result = api_request(
        socket,
        &format!(
            r#"{{"id":"input","method":"pane.send_input","params":{{"pane_id":"{pane}","text":"{escaped}","keys":["Enter"]}}}}"#
        ),
    );
    assert!(
        result.get("error").is_none(),
        "pane.send_input failed: {result}"
    );
}

fn pane_text(socket: &Path, pane: &str) -> String {
    let result = api_request(
        socket,
        &format!(
            r#"{{"id":"read","method":"pane.read","params":{{"pane_id":"{pane}","source":"recent","lines":200}}}}"#
        ),
    );
    result
        .pointer("/result/read/text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .into()
}

/// Asks the pane's shell for its tty size until it matches `accept`.
fn wait_for_tty_size(
    socket: &Path,
    pane: &str,
    timeout: Duration,
    accept: impl Fn((u16, u16)) -> bool,
) -> (u16, u16) {
    let deadline = Instant::now() + timeout;
    let mut last = (0, 0);
    while Instant::now() < deadline {
        let marker = format!(
            "SIZE_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        pane_input(socket, pane, &format!("echo {marker}; stty size"));
        let probe_deadline = Instant::now() + Duration::from_secs(3);
        'probe: while Instant::now() < probe_deadline {
            let mut found = false;
            for line in pane_text(socket, pane).lines() {
                if line.contains(&marker) {
                    found = true;
                    continue;
                }
                if found {
                    let mut words = line.split_whitespace();
                    if let (Some(r), Some(c)) = (words.next(), words.next()) {
                        if let (Ok(r), Ok(c)) = (r.parse(), c.parse()) {
                            last = (r, c);
                            if accept(last) {
                                return last;
                            }
                            break 'probe;
                        }
                    }
                }
            }
            thread::sleep(Duration::from_millis(50));
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "pane tty size never matched; last {last:?}: {}",
        pane_text(socket, pane)
    );
}

/// A control stream with a reader thread collecting every line it emits.
struct ControlStream {
    writer: UnixStream,
    lines: Receiver<Value>,
    connection_id: u64,
    control_protocol: u64,
    capabilities: Value,
}

impl ControlStream {
    fn open(socket: &Path, client: Option<&str>) -> Self {
        let mut stream = UnixStream::connect(socket).unwrap();
        let params = match client {
            Some(name) => {
                format!(r#"{{"client":{{"name":"{name}","version":"e2e","protocol":2}}}}"#)
            }
            None => "{}".to_owned(),
        };
        writeln!(
            stream,
            r#"{{"id":"open","method":"control.open","params":{params}}}"#
        )
        .unwrap();
        let reader = stream.try_clone().unwrap();
        let (tx, lines): (Sender<Value>, Receiver<Value>) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if let Ok(value) = serde_json::from_str::<Value>(line.trim()) {
                            if tx.send(value).is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });
        let opened = lines
            .recv_timeout(Duration::from_secs(10))
            .expect("control.open response");
        assert_eq!(opened["result"]["type"], "control_opened", "{opened}");
        Self {
            writer: stream,
            connection_id: opened["result"]["connection_id"].as_u64().unwrap(),
            control_protocol: opened["result"]["control_protocol"].as_u64().unwrap(),
            capabilities: opened["result"]["capabilities"].clone(),
            lines,
        }
    }

    fn send(&mut self, line: &str) {
        writeln!(self.writer, "{line}").unwrap();
    }

    /// The next line matching `accept`, skipping and discarding others.
    fn wait_for(&self, timeout: Duration, accept: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + timeout;
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(remaining) {
                Ok(value) if accept(&value) => return value,
                Ok(value) => seen.push(value),
                Err(_) => break,
            }
        }
        panic!("no matching line within {timeout:?}; saw {seen:?}");
    }

    fn response(&self, id: &str) -> Value {
        self.wait_for(Duration::from_secs(10), |line| line["id"] == id)
    }

    fn request(&mut self, id: &str, method: &str, params: &str) -> Value {
        self.send(&format!(
            r#"{{"id":"{id}","method":"{method}","params":{params}}}"#
        ));
        self.response(id)
    }

    fn drain(&self) -> Vec<Value> {
        self.lines.try_iter().collect()
    }
}

#[test]
fn control_streams_share_a_pane_and_geometry_follows_the_last_interaction() {
    let base = unique_test_dir();
    let config = base.join("config");
    let runtime = base.join("runtime");
    let api = runtime.join("herdr.sock");
    let server = spawn_server(&config, &runtime, &api);
    wait_for_socket(&api, Duration::from_secs(10));
    let (pane, tab) = create_pane(&api, "shared");

    let mut a = ControlStream::open(&api, Some("a"));
    let mut b = ControlStream::open(&api, Some("b"));
    assert_eq!(a.control_protocol, 2);
    assert_eq!(a.capabilities["terminal_control_stream"], 2);
    assert!(a.capabilities["control_features"]
        .as_array()
        .unwrap()
        .iter()
        .any(|feature| feature == "shared_attach"));

    let subscribed = a.request(
        "sub",
        "events.subscribe",
        r#"{"subscriptions":[{"type":"tab.geometry_changed"}]}"#,
    );
    assert_eq!(subscribed["result"]["type"], "subscription_started");

    let attach = format!(r#"{{"target":"{pane}"}}"#);
    let a_attached = a.request("attach", "terminal.attach", &attach);
    assert_eq!(
        a_attached["result"]["type"], "terminal_attached",
        "{a_attached}"
    );
    let a_attach = a_attached["result"]["attach_id"]
        .as_str()
        .unwrap()
        .to_owned();
    a.wait_for(Duration::from_secs(5), |line| {
        line["type"] == "terminal.snapshot"
    });
    let authority = a.wait_for(Duration::from_secs(5), |line| {
        line["type"] == "terminal.authority"
    });
    assert_eq!(authority["answers_queries"], true);

    let b_attached = b.request("attach", "terminal.attach", &attach);
    assert_eq!(
        b_attached["result"]["type"], "terminal_attached",
        "a second stream joins without takeover: {b_attached}"
    );
    b.wait_for(Duration::from_secs(5), |line| {
        line["type"] == "terminal.snapshot"
    });
    let authority = b.wait_for(Duration::from_secs(5), |line| {
        line["type"] == "terminal.authority"
    });
    assert_eq!(authority["answers_queries"], false);
    assert!(
        a.drain()
            .iter()
            .all(|line| line["type"] != "terminal.detached"),
        "sharing evicts nobody"
    );

    let geometry = |cols: u16, rows: u16, claim: bool| {
        format!(
            r#"{{"tab_id":"{tab}","cols":{cols},"rows":{rows},"cell_width_px":8,"cell_height_px":16,"chrome":"none","claim":{claim}}}"#
        )
    };
    let ok = a.request("geo", "tab.set_geometry", &geometry(120, 40, true));
    assert_eq!(ok["result"]["type"], "ok", "{ok}");
    wait_for_tty_size(&api, &pane, Duration::from_secs(10), |size| {
        size == (40, 120)
    });
    let layout = b.wait_for(Duration::from_secs(5), |line| {
        line["type"] == "tab.layout" && line["layout"]["area"]["width"] == 120
    });
    assert_eq!(
        layout["layout"]["geometry_controller"]["connection_id"],
        a.connection_id
    );

    let ok = b.request("geo", "tab.set_geometry", &geometry(100, 30, true));
    assert_eq!(ok["result"]["type"], "ok", "{ok}");
    wait_for_tty_size(&api, &pane, Duration::from_secs(10), |size| {
        size == (30, 100)
    });
    let changed = a.wait_for(Duration::from_secs(5), |line| {
        line["event"] == "tab_geometry_changed"
            && line["data"]["geometry_controller"]["connection_id"] == b.connection_id
    });
    assert_eq!(
        changed["data"]["previous"]["connection_id"],
        a.connection_id
    );
    assert_eq!(changed["data"]["tab_id"], tab);

    // Typing on the stream that lost the tab takes it back at its size.
    let ok = a.request(
        "type",
        "terminal.input",
        &format!(r#"{{"attach_id":"{a_attach}","bytes":"Cg=="}}"#),
    );
    assert_eq!(ok["result"]["type"], "ok", "{ok}");
    wait_for_tty_size(&api, &pane, Duration::from_secs(10), |size| {
        size == (40, 120)
    });
    a.wait_for(Duration::from_secs(5), |line| {
        line["event"] == "tab_geometry_changed"
            && line["data"]["geometry_controller"]["connection_id"] == a.connection_id
    });

    let listed = a.request("list", "control.list", "{}");
    assert_eq!(listed["result"]["type"], "control_list");
    assert_eq!(listed["result"]["self_connection_id"], a.connection_id);
    let connections = listed["result"]["connections"].as_array().unwrap();
    assert_eq!(connections.len(), 2, "{listed}");
    let mine = connections
        .iter()
        .find(|connection| connection["connection_id"] == a.connection_id)
        .unwrap();
    assert_eq!(mine["client"]["name"], "a");
    assert_eq!(mine["attaches"][0]["attach_id"], a_attach);
    assert_eq!(mine["tabs"][0]["controller"], true);
    let theirs = connections
        .iter()
        .find(|connection| connection["connection_id"] == b.connection_id)
        .unwrap();
    assert_eq!(theirs["tabs"][0]["controller"], false);

    // The owner leaving hands the tab to the other stream's stored size.
    drop(a);
    wait_for_tty_size(&api, &pane, Duration::from_secs(10), |size| {
        size == (30, 100)
    });
    let layout = b.wait_for(Duration::from_secs(5), |line| {
        line["type"] == "tab.layout"
            && line["layout"]["geometry_controller"]["connection_id"] == b.connection_id
    });
    assert_eq!(layout["layout"]["area"]["height"], 30);
    let listed = b.request("list", "control.list", "{}");
    assert_eq!(listed["result"]["connections"].as_array().unwrap().len(), 1);

    // A protocol 1 stream taking over evicts nothing on protocol 2.
    let mut legacy = ControlStream::open(&api, None);
    assert_eq!(legacy.control_protocol, 1);
    let attached = legacy.request(
        "attach",
        "terminal.attach",
        &format!(r#"{{"target":"{pane}","takeover":true}}"#),
    );
    assert_eq!(
        attached["result"]["type"], "terminal_attached",
        "{attached}"
    );
    legacy.wait_for(Duration::from_secs(5), |line| {
        line["type"] == "terminal.snapshot"
    });
    let ping = b.request("ping", "ping", "{}");
    assert_eq!(ping["result"]["type"], "pong");
    assert!(
        b.drain()
            .iter()
            .all(|line| line["type"] != "terminal.detached"),
        "a v1 takeover leaves protocol 2 attaches alone"
    );

    drop(b);
    drop(legacy);
    drop(server);
    cleanup_test_base(&base);
}
