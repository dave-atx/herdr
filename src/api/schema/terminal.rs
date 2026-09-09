//! Control stream and raw terminal attach types.
//!
//! A control stream is one long-lived socket connection that multiplexes
//! ordinary requests, event subscriptions, and raw terminal output for a
//! client that renders panes with its own terminal emulator.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct ControlOpenParams {}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum TerminalAttachMode {
    /// Stream the pane's raw PTY output bytes.
    #[default]
    Raw,
}

/// Which terminal emulator answers queries the pane application sends
/// (device attributes, XTGETTCAP, OSC color queries).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum TerminalQueryAuthority {
    /// The attaching client answers; Herdr suppresses its own replies while
    /// the attach owns the terminal.
    #[default]
    Client,
    /// Herdr keeps answering as it does for every other client.
    Server,
}

/// Who decides the pane's size while the attach owns the terminal.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum TerminalAttachGeometry {
    /// Sizes follow the tab layout; the client drives it with `tab.set_geometry`.
    #[default]
    Tab,
    /// The client sizes this one terminal with `terminal.resize` and Herdr
    /// locks the tab layout out of it, like a direct terminal attach.
    Terminal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalAttachParams {
    /// Pane id, terminal id, or agent name.
    pub target: String,
    #[serde(default)]
    pub mode: TerminalAttachMode,
    /// Upper bound on primary-screen history bytes in the snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_limit_bytes: Option<u64>,
    #[serde(default)]
    pub answer_queries: TerminalQueryAuthority,
    #[serde(default)]
    pub takeover: bool,
    #[serde(default)]
    pub geometry: TerminalAttachGeometry,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cols: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u16>,
    #[serde(default)]
    pub cell_width_px: u32,
    #[serde(default)]
    pub cell_height_px: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalAttachTarget {
    pub attach_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalInputParams {
    pub attach_id: String,
    /// Base64 bytes written to the PTY verbatim.
    pub bytes: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalResizeParams {
    pub attach_id: String,
    pub cols: u16,
    pub rows: u16,
    #[serde(default)]
    pub cell_width_px: u32,
    #[serde(default)]
    pub cell_height_px: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TabSetGeometryParams {
    pub tab_id: String,
    pub cols: u16,
    pub rows: u16,
    #[serde(default)]
    pub cell_width_px: u32,
    #[serde(default)]
    pub cell_height_px: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TerminalScreenKind {
    Primary,
    Alternate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalCursorInfo {
    pub x: u16,
    pub y: u16,
    pub visible: bool,
    /// DECSCUSR parameter (0 to 6).
    pub shape: u8,
}

/// Terminal facts captured with the snapshot. Everything here is also
/// encoded in `state_ansi`; the fields exist so a client can read them
/// without parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct TerminalStateInfo {
    pub cols: u16,
    pub rows: u16,
    pub application_cursor: bool,
    pub bracketed_paste: bool,
    pub focus_reporting: bool,
    pub mouse_reporting: bool,
    pub sgr_pixel_mouse: bool,
    pub mouse_alternate_scroll: bool,
    pub synchronized_output: bool,
    pub color_scheme_reporting: bool,
    pub kitty_keyboard_flags: u16,
    pub modify_other_keys_level: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scroll: Option<super::panes::PaneScrollInfo>,
}

/// Atomic capture of a terminal taken at attach or on request. Output
/// records with `seq` greater than this snapshot's `seq` were produced
/// after it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalSnapshot {
    pub seq: u64,
    pub active_screen: TerminalScreenKind,
    /// Primary screen scrollback plus screen as unwrapped ANSI. Absent when
    /// the alternate screen is active, because only the active screen can
    /// be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<String>,
    /// Alternate screen contents as ANSI when it is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternate: Option<String>,
    /// Mode and keyboard-protocol sequences that restore terminal state.
    pub state_ansi: String,
    pub cursor: TerminalCursorInfo,
    pub state: TerminalStateInfo,
    /// True when `history_limit_bytes` cut older history.
    pub truncated: bool,
}

/// Records pushed on a control stream outside request/response pairs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type")]
pub enum ControlRecord {
    #[serde(rename = "terminal.snapshot")]
    Snapshot {
        attach_id: String,
        snapshot: TerminalSnapshot,
    },
    #[serde(rename = "terminal.output")]
    Output {
        attach_id: String,
        seq: u64,
        /// Base64 raw PTY bytes.
        bytes: String,
    },
    /// Output was dropped because the client fell behind; request
    /// `terminal.snapshot` to resynchronize.
    #[serde(rename = "terminal.gap")]
    Gap {
        attach_id: String,
        seq: u64,
        dropped_bytes: u64,
    },
    #[serde(rename = "terminal.detached")]
    Detached {
        attach_id: String,
        reason: TerminalDetachReason,
    },
    /// A tab's pane rectangles as the terminals were (or are about to be)
    /// sized, ordered on the stream ahead of any output drawn at the new
    /// size. Sent for every tab geometry change while a control stream is
    /// open, whichever client caused it.
    #[serde(rename = "tab.layout")]
    TabLayout {
        layout: super::panes::PaneLayoutSnapshot,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TerminalDetachReason {
    Takeover,
    Closed,
}
