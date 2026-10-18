// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

#[derive(Debug, Clone)]
pub enum ControlAction {
    Conn(bool),
    WMaxLimPct(f32),
    WMaxLimEna(bool),
}

impl ControlAction {
    // Maps a control name (as used in MQTT topics and IPC requests) and its
    // textual value to an action. Returns None for unknown names or invalid values.
    pub fn parse(name: &str, value: &str) -> Option<Self> {
        match name {
            "Conn" => parse_bool(value).map(ControlAction::Conn),
            "WMaxLimPct" => value.parse::<f32>().ok().map(ControlAction::WMaxLimPct),
            "WMaxLim_Ena" => parse_bool(value).map(ControlAction::WMaxLimEna),
            _ => None,
        }
    }
}

pub fn parse_bool(value: &str) -> Option<bool> {
    if value.eq_ignore_ascii_case("true") || value == "1" || value.eq_ignore_ascii_case("on") {
        Some(true)
    } else if value.eq_ignore_ascii_case("false")
        || value == "0"
        || value.eq_ignore_ascii_case("off")
    {
        Some(false)
    } else {
        None
    }
}

pub type CommandResult = std::result::Result<(), String>;

// Reply handle for a command. Commands are distributed via a broadcast channel,
// which requires Clone, so the oneshot sender is shared and taken on first use.
// If no connection task owns the target device, all clones are dropped and the
// requester's receiver observes a RecvError.
#[derive(Clone)]
pub struct CommandReply(Arc<Mutex<Option<oneshot::Sender<CommandResult>>>>);

impl CommandReply {
    pub fn new() -> (Self, oneshot::Receiver<CommandResult>) {
        let (tx, rx) = oneshot::channel();
        (Self(Arc::new(Mutex::new(Some(tx)))), rx)
    }

    pub fn send(&self, result: CommandResult) {
        let sender = self.0.lock().ok().and_then(|mut s| s.take());
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
    }
}

impl std::fmt::Debug for CommandReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CommandReply")
    }
}

#[derive(Debug, Clone)]
pub struct ModbusCommand {
    pub serial: String,
    pub action: ControlAction,
    pub reply: Option<CommandReply>,
}

impl ModbusCommand {
    pub fn respond(&self, result: CommandResult) {
        if let Some(reply) = &self.reply {
            reply.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_action_parse() {
        assert!(matches!(
            ControlAction::parse("Conn", "on"),
            Some(ControlAction::Conn(true))
        ));
        assert!(matches!(
            ControlAction::parse("WMaxLim_Ena", "0"),
            Some(ControlAction::WMaxLimEna(false))
        ));
        assert!(matches!(
            ControlAction::parse("WMaxLimPct", "42.5"),
            Some(ControlAction::WMaxLimPct(v)) if v == 42.5
        ));
        assert!(ControlAction::parse("WMaxLimPct", "abc").is_none());
        assert!(ControlAction::parse("Conn", "maybe").is_none());
        assert!(ControlAction::parse("Unknown", "1").is_none());
    }

    #[tokio::test]
    async fn test_command_reply_first_send_wins() {
        let (reply, rx) = CommandReply::new();
        let clone = reply.clone();
        reply.send(Ok(()));
        clone.send(Err("ignored".to_string()));
        assert_eq!(rx.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn test_command_reply_dropped() {
        let (reply, rx) = CommandReply::new();
        drop(reply);
        assert!(rx.await.is_err());
    }
}
