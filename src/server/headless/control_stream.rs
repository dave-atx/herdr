//! Server side of control streams: raw terminal attaches, their ownership,
//! and tab geometry claims made by a client that renders panes itself.

use std::collections::HashMap;
use std::sync::Arc;

use ratatui::layout::Rect;
use tracing::info;

use crate::api;
use crate::api::control::ControlConnectionHandle;
use crate::api::schema::{
    ErrorBody, ErrorResponse, ResponseResult, SuccessResponse, TabSetGeometryParams,
    TerminalAttachGeometry, TerminalAttachParams, TerminalAttachTarget, TerminalDetachReason,
    TerminalQueryAuthority, TerminalResizeParams,
};
use crate::pane::raw_stream::{RawTapBudget, DEFAULT_TAP_BUDGET_BYTES};
use crate::protocol::ServerMessage;

use super::HeadlessServer;

/// Control connection ids live above every client id so ownership maps can
/// hold both.
const CONTROL_CONNECTION_ID_BASE: u64 = 1 << 40;
const DEFAULT_HISTORY_LIMIT_BYTES: usize = 1024 * 1024;
const MAX_HISTORY_LIMIT_BYTES: usize = 8 * 1024 * 1024;

pub(super) fn is_control_connection_id(id: u64) -> bool {
    id >= CONTROL_CONNECTION_ID_BASE
}

pub(super) struct ControlConnectionState {
    handle: ControlConnectionHandle,
    attaches: HashMap<String, ControlAttach>,
    /// Requested size per tab id this connection controls.
    tab_geometry: HashMap<String, (u16, u16, crate::kitty_graphics::HostCellSize)>,
    next_attach: u64,
}

impl ControlConnectionState {
    pub(super) fn holds_tab(&self, tab_id: &str) -> bool {
        self.tab_geometry.contains_key(tab_id)
    }

    pub(super) fn has_tab_claims(&self) -> bool {
        !self.tab_geometry.is_empty()
    }

    pub(super) fn tab_geometry(
        &self,
        tab_id: &str,
    ) -> Option<(u16, u16, crate::kitty_graphics::HostCellSize)> {
        self.tab_geometry.get(tab_id).copied()
    }
}

struct ControlAttach {
    terminal_id: String,
    geometry: TerminalAttachGeometry,
    history_limit_bytes: usize,
}

fn error(id: String, code: &str, message: String) -> String {
    serde_json::to_string(&ErrorResponse {
        id,
        error: ErrorBody {
            code: code.into(),
            message,
        },
    })
    .unwrap_or_else(|_| "{}".to_string())
}

fn success(id: String, result: ResponseResult) -> String {
    serde_json::to_string(&SuccessResponse { id, result }).unwrap_or_else(|_| "{}".to_string())
}

impl HeadlessServer {
    /// Handles control-stream methods before the app sees them. Returns the
    /// response when the request was one of them.
    pub(super) fn handle_control_api_request(
        &mut self,
        msg: &api::ApiRequestMessage,
    ) -> Option<String> {
        use api::schema::Method;

        let id = msg.request.id.clone();
        let response = match &msg.request.method {
            Method::ControlOpen(_) => self.control_open(id, msg.control.as_ref()),
            Method::ControlClose(_) => self.control_close(id, msg.control.as_ref()),
            Method::TerminalAttach(params) => {
                self.control_terminal_attach(id, msg.control.as_ref(), params)
            }
            Method::TerminalDetach(params) => {
                self.control_terminal_detach(id, msg.control.as_ref(), params)
            }
            Method::TerminalSnapshot(params) => {
                self.control_terminal_snapshot(id, msg.control.as_ref(), params)
            }
            Method::TerminalResize(params) => {
                self.control_terminal_resize(id, msg.control.as_ref(), params)
            }
            Method::TerminalInput(_) => error(
                id,
                "control_stream_required",
                "terminal.input is only available on a control stream".into(),
            ),
            Method::TabSetGeometry(params) => {
                self.control_tab_set_geometry(id, msg.control.as_ref(), params)
            }
            _ => return None,
        };
        Some(response)
    }

    fn control_connection_id(
        &mut self,
        handle: Option<&ControlConnectionHandle>,
    ) -> Result<u64, String> {
        let Some(handle) = handle else {
            return Err("this method is only available on a control stream".into());
        };
        let id = handle.id();
        if id == 0 || !self.control_connections.contains_key(&id) {
            return Err("control stream is not open".into());
        }
        self.prune_closed_control_attaches(id);
        Ok(id)
    }

    fn control_open(&mut self, id: String, handle: Option<&ControlConnectionHandle>) -> String {
        let Some(handle) = handle else {
            return error(
                id,
                "control_stream_required",
                "control.open must be the first request on its own connection".into(),
            );
        };
        if handle.id() != 0 {
            return error(
                id,
                "unsupported_in_control_stream",
                "control stream is already open".into(),
            );
        }
        let connection_id = CONTROL_CONNECTION_ID_BASE + self.next_control_connection_id;
        self.next_control_connection_id += 1;
        handle.assign_id(connection_id);
        self.control_connections.insert(
            connection_id,
            ControlConnectionState {
                handle: handle.clone(),
                attaches: HashMap::new(),
                tab_geometry: HashMap::new(),
                next_attach: 0,
            },
        );
        info!(connection_id, "control stream opened");
        success(
            id,
            ResponseResult::ControlOpened {
                connection_id,
                boot_id: self.client_shell_boot_id.clone(),
                version: crate::build_info::version(),
                protocol: crate::protocol::PROTOCOL_VERSION,
                capabilities: api::default_server_capabilities(),
            },
        )
    }

    fn control_close(&mut self, id: String, handle: Option<&ControlConnectionHandle>) -> String {
        let Some(handle) = handle else {
            return error(
                id,
                "control_stream_required",
                "control.close is only available on a control stream".into(),
            );
        };
        let connection_id = handle.id();
        if self.control_connections.contains_key(&connection_id) {
            self.release_control_connection(connection_id);
            info!(connection_id, "control stream closed");
        }
        success(id, ResponseResult::Ok {})
    }

    /// Drops every attach and geometry claim of a control connection.
    pub(super) fn release_control_connection(&mut self, connection_id: u64) {
        let Some(state) = self.control_connections.remove(&connection_id) else {
            return;
        };
        state.handle.close();
        for (attach_id, attach) in state.attaches {
            self.release_control_attach(
                connection_id,
                &state.handle,
                &attach_id,
                &attach,
                TerminalDetachReason::Closed,
            );
        }
        // Only claims this connection still holds: another stream may have
        // taken a tab over since, and its chrome choice stays in force.
        let owned = state
            .tab_geometry
            .keys()
            .filter(|tab_id| self.tab_geometry_controllers.get(*tab_id) == Some(&connection_id))
            .cloned()
            .collect::<Vec<_>>();
        self.tab_geometry_controllers
            .retain(|_, controller| *controller != connection_id);
        self.sync_control_geometry_tabs();
        for tab_id in owned {
            self.app.state.control_chromeless_tabs.remove(&tab_id);
        }
        // Always re-derive geometry: terminal-sized attaches released above
        // need their tabs back even when this stream held no tab claims.
        if !self.resize_tabs_for_only_shell_client(true) {
            self.reapply_controlled_shell_tab_geometry(true);
        }
    }

    fn release_control_attach(
        &mut self,
        connection_id: u64,
        handle: &ControlConnectionHandle,
        attach_id: &str,
        attach: &ControlAttach,
        reason: TerminalDetachReason,
    ) {
        handle.unregister_input(attach_id);
        if let Some(runtime) = self.control_runtime(&attach.terminal_id) {
            runtime.detach_raw(attach_id, reason);
        }
        if self.terminal_attach_owners.get(&attach.terminal_id) == Some(&connection_id) {
            self.terminal_attach_owners.remove(&attach.terminal_id);
        }
        if attach.geometry == TerminalAttachGeometry::Terminal {
            if let Some(terminal_id) = self.terminal_id_by_string(&attach.terminal_id) {
                self.app
                    .state
                    .direct_attach_resize_locks
                    .remove(&terminal_id);
            }
            // Hand the terminal back to whoever sizes its tab, as a direct
            // attach client's removal does.
            if let Some((controller_id, target)) =
                self.shell_geometry_controller_for_terminal(&attach.terminal_id)
            {
                self.restore_shell_tab_geometry(controller_id, target);
            } else {
                self.resize_tabs_for_only_shell_client(true);
            }
        }
    }

    /// Forget attaches whose terminal went away; the pane already sent
    /// `terminal.detached` when its runtime dropped.
    fn prune_closed_control_attaches(&mut self, connection_id: u64) {
        let Some(state) = self.control_connections.get(&connection_id) else {
            return;
        };
        let gone = state
            .attaches
            .iter()
            .filter(|(_, attach)| self.control_runtime(&attach.terminal_id).is_none())
            .map(|(attach_id, _)| attach_id.clone())
            .collect::<Vec<_>>();
        for attach_id in gone {
            let Some(state) = self.control_connections.get_mut(&connection_id) else {
                return;
            };
            let Some(attach) = state.attaches.remove(&attach_id) else {
                continue;
            };
            let handle = state.handle.clone();
            handle.unregister_input(&attach_id);
            if self.terminal_attach_owners.get(&attach.terminal_id) == Some(&connection_id) {
                self.terminal_attach_owners.remove(&attach.terminal_id);
            }
        }
    }

    /// Detaches every control attach on a terminal, for a takeover by
    /// another attach or a direct terminal client.
    pub(super) fn detach_control_attaches_for_terminal(
        &mut self,
        terminal_id: &str,
        reason: TerminalDetachReason,
    ) {
        let targets = self
            .control_connections
            .iter()
            .flat_map(|(&connection_id, state)| {
                state
                    .attaches
                    .iter()
                    .filter(|(_, attach)| attach.terminal_id == terminal_id)
                    .map(move |(attach_id, _)| (connection_id, attach_id.clone()))
            })
            .collect::<Vec<_>>();
        for (connection_id, attach_id) in targets {
            let Some(state) = self.control_connections.get_mut(&connection_id) else {
                continue;
            };
            let Some(attach) = state.attaches.remove(&attach_id) else {
                continue;
            };
            let handle = state.handle.clone();
            self.release_control_attach(connection_id, &handle, &attach_id, &attach, reason);
        }
    }

    fn control_terminal_attach(
        &mut self,
        id: String,
        handle: Option<&ControlConnectionHandle>,
        params: &TerminalAttachParams,
    ) -> String {
        let connection_id = match self.control_connection_id(handle) {
            Ok(connection_id) => connection_id,
            Err(message) => return error(id, "control_stream_required", message),
        };
        let (terminal_id, pane_id) = match self.app.resolve_terminal_target(&params.target) {
            Ok(target) => (
                target.terminal_id,
                self.app.public_pane_id(target.ws_idx, target.pane_id),
            ),
            Err(crate::app::terminal_targets::TerminalTargetError::Ambiguous {
                target, ..
            }) => {
                return error(
                    id,
                    "ambiguous_target",
                    format!("terminal target {target} matches more than one terminal"),
                );
            }
            Err(crate::app::terminal_targets::TerminalTargetError::NotFound { .. }) => {
                if self.control_runtime(&params.target).is_some() {
                    (params.target.clone(), None)
                } else {
                    return error(
                        id,
                        "not_found",
                        format!("terminal target {} not found", params.target),
                    );
                }
            }
        };
        if self
            .pending_alt_screen_reads
            .iter()
            .any(|pending| pending.terminal_id.to_string() == terminal_id)
        {
            return error(
                id,
                "terminal_busy",
                format!("terminal {terminal_id} has a read in progress; retry"),
            );
        }

        if let Some(owner) = self.terminal_attach_owners.get(&terminal_id).copied() {
            if owner != connection_id && !params.takeover {
                return error(
                    id,
                    "terminal_attached",
                    format!(
                        "terminal {terminal_id} already has an attached client; retry with takeover"
                    ),
                );
            }
            if is_control_connection_id(owner) {
                self.detach_control_attaches_for_terminal(
                    &terminal_id,
                    TerminalDetachReason::Takeover,
                );
            } else {
                self.send_to_client(
                    owner,
                    ServerMessage::ServerShutdown {
                        reason: Some("terminal attach taken over".to_owned()),
                    },
                );
                self.remove_client_and_resize_if_needed(owner);
            }
        }

        let Some(handle) = handle.cloned() else {
            return error(
                id,
                "control_stream_required",
                "terminal.attach is only available on a control stream".into(),
            );
        };
        let Some(state) = self.control_connections.get_mut(&connection_id) else {
            return error(
                id,
                "control_stream_required",
                "control stream is not open".into(),
            );
        };
        let attach_id = format!(
            "{}-{}",
            connection_id - CONTROL_CONNECTION_ID_BASE,
            state.next_attach
        );
        state.next_attach += 1;
        let history_limit_bytes = params
            .history_limit_bytes
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX))
            .unwrap_or(DEFAULT_HISTORY_LIMIT_BYTES)
            .min(MAX_HISTORY_LIMIT_BYTES);
        let suppress = params.answer_queries == TerminalQueryAuthority::Client;
        // Bookkeeping first: the runtime borrow below must not overlap it.
        state.attaches.insert(
            attach_id.clone(),
            ControlAttach {
                terminal_id: terminal_id.clone(),
                geometry: params.geometry,
                history_limit_bytes,
            },
        );
        self.terminal_attach_owners
            .insert(terminal_id.clone(), connection_id);
        if params.geometry == TerminalAttachGeometry::Terminal {
            if let Some(real_terminal_id) = self.terminal_id_by_string(&terminal_id) {
                self.app
                    .state
                    .direct_attach_resize_locks
                    .insert(real_terminal_id);
            }
        }
        let Some(runtime) = self.control_runtime(&terminal_id) else {
            self.terminal_attach_owners.remove(&terminal_id);
            if let Some(state) = self.control_connections.get_mut(&connection_id) {
                state.attaches.remove(&attach_id);
            }
            return error(id, "not_found", format!("terminal {terminal_id} not found"));
        };
        runtime.attach_raw(
            attach_id.clone(),
            handle.outbound(),
            Arc::new(RawTapBudget::new(DEFAULT_TAP_BUDGET_BYTES)),
            suppress,
        );
        handle.register_input(attach_id.clone(), runtime.raw_input_sink());
        if params.geometry == TerminalAttachGeometry::Terminal {
            if let (Some(cols), Some(rows)) = (params.cols, params.rows) {
                runtime.resize(rows, cols, params.cell_width_px, params.cell_height_px);
            }
        }
        info!(connection_id, terminal_id = %terminal_id, attach_id = %attach_id, "control stream attached");
        // Write the response through the stream ourselves so the client
        // learns the attach id before the snapshot record that follows it.
        handle.send_line(success(
            id,
            ResponseResult::TerminalAttached {
                attach_id: attach_id.clone(),
                terminal_id: terminal_id.clone(),
                pane_id,
            },
        ));
        if !runtime.snapshot_raw(&attach_id, history_limit_bytes) {
            handle.send_line(error(
                String::new(),
                "snapshot_failed",
                format!("terminal {terminal_id} could not be snapshotted"),
            ));
        }
        String::new()
    }

    fn control_terminal_detach(
        &mut self,
        id: String,
        handle: Option<&ControlConnectionHandle>,
        params: &TerminalAttachTarget,
    ) -> String {
        let connection_id = match self.control_connection_id(handle) {
            Ok(connection_id) => connection_id,
            Err(message) => return error(id, "control_stream_required", message),
        };
        let Some(state) = self.control_connections.get_mut(&connection_id) else {
            return error(
                id,
                "control_stream_required",
                "control stream is not open".into(),
            );
        };
        let Some(attach) = state.attaches.remove(&params.attach_id) else {
            return error(
                id,
                "unknown_attach",
                format!("attach {} is not live", params.attach_id),
            );
        };
        let handle = state.handle.clone();
        self.release_control_attach(
            connection_id,
            &handle,
            &params.attach_id,
            &attach,
            TerminalDetachReason::Closed,
        );
        success(id, ResponseResult::Ok {})
    }

    fn control_terminal_snapshot(
        &mut self,
        id: String,
        handle: Option<&ControlConnectionHandle>,
        params: &TerminalAttachTarget,
    ) -> String {
        let connection_id = match self.control_connection_id(handle) {
            Ok(connection_id) => connection_id,
            Err(message) => return error(id, "control_stream_required", message),
        };
        let Some((terminal_id, history_limit_bytes)) = self
            .control_connections
            .get(&connection_id)
            .and_then(|state| state.attaches.get(&params.attach_id))
            .map(|attach| (attach.terminal_id.clone(), attach.history_limit_bytes))
        else {
            return error(
                id,
                "unknown_attach",
                format!("attach {} is not live", params.attach_id),
            );
        };
        let Some(runtime) = self.control_runtime(&terminal_id) else {
            return error(id, "not_found", format!("terminal {terminal_id} not found"));
        };
        if !runtime.snapshot_raw(&params.attach_id, history_limit_bytes) {
            return error(
                id,
                "snapshot_failed",
                format!("terminal {terminal_id} could not be snapshotted"),
            );
        }
        success(id, ResponseResult::Ok {})
    }

    fn control_terminal_resize(
        &mut self,
        id: String,
        handle: Option<&ControlConnectionHandle>,
        params: &TerminalResizeParams,
    ) -> String {
        let connection_id = match self.control_connection_id(handle) {
            Ok(connection_id) => connection_id,
            Err(message) => return error(id, "control_stream_required", message),
        };
        let Some((terminal_id, geometry)) = self
            .control_connections
            .get(&connection_id)
            .and_then(|state| state.attaches.get(&params.attach_id))
            .map(|attach| (attach.terminal_id.clone(), attach.geometry))
        else {
            return error(
                id,
                "unknown_attach",
                format!("attach {} is not live", params.attach_id),
            );
        };
        if geometry != TerminalAttachGeometry::Terminal {
            return error(
                id,
                "geometry_follows_tab",
                "this attach follows its tab layout; use tab.set_geometry".into(),
            );
        }
        if params.cols == 0 || params.rows == 0 {
            return error(
                id,
                "invalid_request",
                "terminal.resize cols and rows must be greater than 0".into(),
            );
        }
        let Some(runtime) = self.control_runtime(&terminal_id) else {
            return error(id, "not_found", format!("terminal {terminal_id} not found"));
        };
        runtime.resize(
            params.rows,
            params.cols,
            params.cell_width_px,
            params.cell_height_px,
        );
        success(id, ResponseResult::Ok {})
    }

    fn control_tab_set_geometry(
        &mut self,
        id: String,
        handle: Option<&ControlConnectionHandle>,
        params: &TabSetGeometryParams,
    ) -> String {
        let connection_id = match self.control_connection_id(handle) {
            Ok(connection_id) => connection_id,
            Err(message) => return error(id, "control_stream_required", message),
        };
        if params.cols == 0 || params.rows == 0 {
            return error(
                id,
                "invalid_request",
                "tab.set_geometry cols and rows must be greater than 0".into(),
            );
        }
        let Some((workspace_index, tab_index)) = self.app.parse_tab_id(&params.tab_id) else {
            return error(id, "not_found", format!("tab {} not found", params.tab_id));
        };
        let cell_size = crate::kitty_graphics::HostCellSize {
            width_px: params.cell_width_px,
            height_px: params.cell_height_px,
        };
        let Some(state) = self.control_connections.get_mut(&connection_id) else {
            return error(
                id,
                "control_stream_required",
                "control stream is not open".into(),
            );
        };
        state
            .tab_geometry
            .insert(params.tab_id.clone(), (params.cols, params.rows, cell_size));
        self.tab_geometry_controllers
            .insert(params.tab_id.clone(), connection_id);
        self.sync_control_geometry_tabs();
        match params.chrome {
            api::schema::TabChrome::None => {
                self.app
                    .state
                    .control_chromeless_tabs
                    .insert(params.tab_id.clone());
            }
            api::schema::TabChrome::Server => {
                self.app
                    .state
                    .control_chromeless_tabs
                    .remove(&params.tab_id);
            }
        }
        self.apply_control_tab_geometry(
            crate::ui::TabSurfaceTarget {
                workspace_index,
                tab_index,
            },
            params.cols,
            params.rows,
            cell_size,
        );
        success(id, ResponseResult::Ok {})
    }

    /// Resolves a terminal id string to its runtime through the pane path
    /// first, which also serves test workspaces; popup terminals fall back to
    /// the terminal registry.
    fn control_runtime(&self, terminal_id: &str) -> Option<&crate::terminal::TerminalRuntime> {
        if let Ok(target) = self.app.resolve_terminal_target(terminal_id) {
            if let Some(runtime) = self.app.state.runtime_for_pane_in_workspace(
                &self.app.terminal_runtimes,
                target.ws_idx,
                target.pane_id,
            ) {
                return Some(runtime);
            }
        }
        self.runtime_for_terminal_id_string(terminal_id)
    }

    pub(super) fn apply_control_tab_geometry(
        &mut self,
        target: crate::ui::TabSurfaceTarget,
        cols: u16,
        rows: u16,
        cell_size: crate::kitty_graphics::HostCellSize,
    ) {
        let area = Rect::new(0, 0, cols, rows);
        self.with_tab_layout_boundary(&[target], area, |server| {
            crate::ui::compute_tab_surface_for(
                &server.app.state,
                &server.app.terminal_runtimes,
                Some(target),
                area,
                true,
                cell_size,
            );
        });
        self.finish_shell_tab_geometry_change(true);
    }

    pub(super) fn all_tab_targets(&self) -> Vec<crate::ui::TabSurfaceTarget> {
        self.app
            .state
            .workspaces
            .iter()
            .enumerate()
            .flat_map(|(workspace_index, workspace)| {
                (0..workspace.tabs.len()).map(move |tab_index| crate::ui::TabSurfaceTarget {
                    workspace_index,
                    tab_index,
                })
            })
            .collect()
    }

    /// Lays `targets` out in `area` behind one boundary: every pane's
    /// content lock is held while the `tab.layout` records go out and
    /// `resize` runs, so no PTY read can publish old-size output after the
    /// record or new-size output before it. Shell-only servers just resize.
    pub(super) fn with_tab_layout_boundary(
        &self,
        targets: &[crate::ui::TabSurfaceTarget],
        area: Rect,
        resize: impl FnOnce(&Self),
    ) {
        if self.control_connections.is_empty() {
            resize(self);
            return;
        }
        let holds = targets
            .iter()
            .filter_map(|target| {
                let tab = self
                    .app
                    .state
                    .workspaces
                    .get(target.workspace_index)?
                    .tabs
                    .get(target.tab_index)?;
                Some((target.workspace_index, tab.layout.pane_ids()))
            })
            .flat_map(|(workspace_index, pane_ids)| {
                pane_ids.into_iter().filter_map(move |pane_id| {
                    self.app.state.runtime_for_pane_in_workspace(
                        &self.app.terminal_runtimes,
                        workspace_index,
                        pane_id,
                    )
                })
            })
            .map(|runtime| runtime.hold_content_write_lock())
            .collect::<Vec<_>>();
        for target in targets {
            self.push_control_tab_layout(*target, area);
        }
        resize(self);
        drop(holds);
    }

    /// Sends a `tab.layout` record for the tab about to be laid out in
    /// `area` to every control stream. The record travels the same ordered
    /// lane as raw output; `with_tab_layout_boundary` makes that order hold.
    fn push_control_tab_layout(&self, target: crate::ui::TabSurfaceTarget, area: Rect) {
        let surface = crate::ui::compute_tab_surface_for(
            &self.app.state,
            &self.app.terminal_runtimes,
            Some(target),
            area,
            false,
            crate::kitty_graphics::HostCellSize::default(),
        );
        let Some(layout) = self.app.control_tab_layout_snapshot(
            target.workspace_index,
            target.tab_index,
            area,
            &surface.pane_infos,
        ) else {
            return;
        };
        let Ok(line) = serde_json::to_string(&api::schema::ControlRecord::TabLayout { layout })
        else {
            return;
        };
        for state in self.control_connections.values() {
            state.handle.send_line(line.clone());
        }
    }

    /// Size a control connection asked for on a tab, when it controls it.
    pub(super) fn control_tab_geometry_for_target(
        &self,
        controller_id: u64,
        target: crate::ui::TabSurfaceTarget,
    ) -> Option<(u16, u16, crate::kitty_graphics::HostCellSize)> {
        if !is_control_connection_id(controller_id) {
            return None;
        }
        let tab_id = self.tab_id_for_target(target)?;
        self.control_connections
            .get(&controller_id)?
            .tab_geometry(&tab_id)
    }

    /// Mirrors control-owned claims into `AppState` for `crate::ui`. Call
    /// after every mutation of `tab_geometry_controllers`.
    pub(super) fn sync_control_geometry_tabs(&mut self) {
        self.app.state.control_geometry_tabs = self
            .tab_geometry_controllers
            .iter()
            .filter(|(_, controller)| is_control_connection_id(**controller))
            .map(|(tab_id, _)| tab_id.clone())
            .collect();
    }

    pub(super) fn control_connection_holds_tab(&self, controller_id: u64, tab_id: &str) -> bool {
        self.control_connections
            .get(&controller_id)
            .is_some_and(|state| state.holds_tab(tab_id))
    }

    pub(super) fn control_connections_with_tab_claims(&self) -> usize {
        self.control_connections
            .values()
            .filter(|state| state.has_tab_claims())
            .count()
    }

    pub(super) fn control_connection_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.control_connections.keys().copied()
    }
}
