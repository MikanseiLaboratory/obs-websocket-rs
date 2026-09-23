//! Cached OBS state, updated from events and an initial snapshot.

use std::collections::BTreeMap;

use obs_websocket_core::Event;

/// Mute and volume for one input, keyed by input name in [`ObsState::inputs`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct InputState {
    /// Last known mute flag.
    pub muted: Option<bool>,
    /// Last known linear volume.
    pub volume_mul: Option<f64>,
    /// Last known volume in dB.
    pub volume_db: Option<f64>,
}

/// A best-effort view of the OBS session.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ObsState {
    /// Current program scene name.
    pub program_scene: Option<String>,
    /// Current program scene UUID.
    pub program_scene_uuid: Option<String>,
    /// Studio mode, when known.
    pub studio_mode: Option<bool>,
    /// Inputs seen in `GetInputList` or volume and mute events.
    pub inputs: BTreeMap<String, InputState>,
    /// Whether streaming output is active.
    pub streaming: Option<bool>,
    /// Whether recording output is active.
    pub recording: Option<bool>,
    /// Whether the virtual camera is active.
    pub virtual_cam: Option<bool>,
}

impl ObsState {
    pub(crate) fn apply(&mut self, event: &Event) {
        match event {
            Event::CurrentProgramSceneChanged(payload) => {
                self.program_scene = Some(payload.scene_name.clone());
                self.program_scene_uuid = Some(payload.scene_uuid.clone());
            }
            Event::StudioModeStateChanged(payload) => {
                self.studio_mode = Some(payload.studio_mode_enabled);
            }
            Event::InputMuteStateChanged(payload) => {
                self.inputs
                    .entry(payload.input_name.clone())
                    .or_default()
                    .muted = Some(payload.input_muted);
            }
            Event::InputVolumeChanged(payload) => {
                let input = self.inputs.entry(payload.input_name.clone()).or_default();
                input.volume_mul = Some(payload.input_volume_mul);
                input.volume_db = Some(payload.input_volume_db);
            }
            Event::StreamStateChanged(payload) => {
                self.streaming = Some(payload.output_active);
            }
            Event::RecordStateChanged(payload) => {
                self.recording = Some(payload.output_active);
            }
            Event::VirtualcamStateChanged(payload) => {
                self.virtual_cam = Some(payload.output_active);
            }
            _ => {}
        }
    }
}
