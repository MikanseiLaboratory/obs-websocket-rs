//! Query and control OBS from the command line.
//!
//! ```text
//! cargo run -p obs-websocket --example cli -- status
//! cargo run -p obs-websocket --example cli -- --help
//! ```
//!
//! The password comes from `--password` or the `OBS_WS_PASSWORD` environment variable.

use std::error::Error;

use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use obs_websocket::{Client, ConnectConfig, ReconnectPolicy};
use obs_websocket_core::requests::{
    GetInputList, GetInputMute, GetInputVolume, GetSceneList, SetCurrentProgramScene, SetInputMute,
    SetInputVolume, TriggerHotkeyByName,
};

#[derive(Parser)]
#[command(
    name = "obs-websocket",
    about = "Query and control OBS over obs-websocket v5"
)]
struct Cli {
    /// OBS host, without a scheme.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// obs-websocket port.
    #[arg(long, default_value_t = 4455)]
    port: u16,
    /// WebSocket password. Defaults to the `OBS_WS_PASSWORD` environment variable.
    #[arg(long, env = "OBS_WS_PASSWORD")]
    password: Option<String>,
    /// Connect with `wss://`. Requires the `rustls` feature, which is on by default.
    #[arg(long)]
    tls: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show the current scene and output status.
    Status,
    /// Show OBS and obs-websocket versions.
    Version,
    /// Show OBS resource statistics.
    Stats,
    /// List or switch scenes.
    Scenes {
        #[command(subcommand)]
        action: ScenesCommand,
    },
    /// List inputs, or change mute and volume.
    Inputs {
        #[command(subcommand)]
        action: InputsCommand,
    },
    /// Read or change the stream output.
    Stream {
        #[command(subcommand)]
        action: StreamCommand,
    },
    /// Read or change the recording output.
    Record {
        #[command(subcommand)]
        action: RecordCommand,
    },
    /// Read or change the virtual camera.
    VirtualCam {
        #[command(subcommand)]
        action: VirtualCamCommand,
    },
    /// Read or change the replay buffer.
    Replay {
        #[command(subcommand)]
        action: ReplayCommand,
    },
    /// List or trigger hotkeys.
    Hotkeys {
        #[command(subcommand)]
        action: HotkeysCommand,
    },
    /// Send any request type. `--data` is a JSON value.
    Raw {
        /// Protocol `requestType`, such as `GetVersion`.
        request_type: String,
        /// Request data JSON. Use `null` when the request has no fields.
        #[arg(long, default_value = "null")]
        data: String,
    },
    /// Print events until the process is interrupted.
    Watch,
}

#[derive(Subcommand)]
enum ScenesCommand {
    /// List scenes. The current program scene is marked.
    List,
    /// Show the current program scene.
    Current,
    /// Switch the program scene.
    Set {
        /// Scene name.
        name: String,
    },
}

#[derive(Subcommand)]
enum InputsCommand {
    /// List input names and kinds.
    List,
    /// Show mute and volume for one input.
    Info {
        /// Input name.
        name: String,
    },
    /// Mute an input.
    Mute { name: String },
    /// Unmute an input.
    Unmute { name: String },
    /// Toggle mute and print the new state.
    Toggle { name: String },
    /// Show volume, or set it with `--mul` or `--db`.
    Volume {
        name: String,
        /// Linear volume, from 0 to 20.
        #[arg(long)]
        mul: Option<f64>,
        /// Volume in dB, from -100 to 26.
        #[arg(long)]
        db: Option<f64>,
    },
}

#[derive(Subcommand)]
enum StreamCommand {
    Status,
    Start,
    Stop,
    Toggle,
}

#[derive(Subcommand)]
enum RecordCommand {
    Status,
    Start,
    Stop,
    Toggle,
    /// Toggle the recording pause state.
    Pause,
}

#[derive(Subcommand)]
enum VirtualCamCommand {
    Status,
    Start,
    Stop,
    Toggle,
}

#[derive(Subcommand)]
enum ReplayCommand {
    Status,
    Start,
    Stop,
    Toggle,
    Save,
}

#[derive(Subcommand)]
enum HotkeysCommand {
    List,
    Trigger { name: String },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn Error>> {
    let password = cli.password.filter(|value| !value.is_empty());
    let client = Client::connect(
        ConnectConfig::new(cli.host, cli.port)
            .password(password.as_deref())
            .tls(cli.tls)
            .reconnect(ReconnectPolicy::disabled()),
    )
    .await?;
    match cli.command {
        Command::Status => status(&client).await,
        Command::Version => print_json(&client.general().get_version().await?),
        Command::Stats => print_json(&client.general().get_stats().await?),
        Command::Scenes { action } => scenes(&client, action).await,
        Command::Inputs { action } => inputs(&client, action).await,
        Command::Stream { action } => stream(&client, action).await,
        Command::Record { action } => record(&client, action).await,
        Command::VirtualCam { action } => virtual_cam(&client, action).await,
        Command::Replay { action } => replay(&client, action).await,
        Command::Hotkeys { action } => hotkeys(&client, action).await,
        Command::Raw { request_type, data } => {
            let value = serde_json::from_str(&data)?;
            print_json(&client.raw_request(request_type, value).await?)
        }
        Command::Watch => {
            let mut events = Box::pin(client.events());
            while let Some(event) = events.next().await {
                println!("{} {event:?}", event.event_type());
            }
            Ok(())
        }
    }
}

async fn status(client: &Client) -> Result<(), Box<dyn Error>> {
    let version = client.general().get_version().await?;
    let scene = client.scenes().get_current_program_scene().await?;
    let studio = client.ui().get_studio_mode_enabled().await?;
    let stream = client.stream().get_stream_status().await?;
    let record = client.record().get_record_status().await?;
    let camera = match client.outputs().get_virtual_cam_status().await {
        Ok(status) => on_off(status.output_active),
        Err(obs_websocket::Error::Request { .. }) => "unavailable",
        Err(error) => return Err(error.into()),
    };
    let replay = match client.outputs().get_replay_buffer_status().await {
        Ok(status) => on_off(status.output_active),
        Err(obs_websocket::Error::Request { .. }) => "unavailable",
        Err(error) => return Err(error.into()),
    };
    println!(
        "OBS {}  (obs-websocket {}, rpc {})",
        version.obs_version, version.obs_web_socket_version, version.rpc_version
    );
    println!("Scene   {}", scene.scene_name);
    println!("Studio  {}", on_off(studio.studio_mode_enabled));
    println!(
        "Stream  {}",
        output_line(stream.output_active, &stream.output_timecode)
    );
    println!(
        "Record  {}",
        if record.output_paused {
            format!("paused  {}", record.output_timecode)
        } else {
            output_line(record.output_active, &record.output_timecode)
        }
    );
    println!("Virtual camera  {camera}");
    println!("Replay buffer   {replay}");
    Ok(())
}

async fn scenes(client: &Client, action: ScenesCommand) -> Result<(), Box<dyn Error>> {
    match action {
        ScenesCommand::List => {
            let list = client.scenes().get_scene_list(&GetSceneList::new()).await?;
            let current = list.current_program_scene_name.as_deref();
            for scene in list.scenes {
                let mark = if Some(scene.scene_name.as_str()) == current {
                    "*"
                } else {
                    " "
                };
                println!("{mark} {}", scene.scene_name);
            }
        }
        ScenesCommand::Current => {
            let scene = client.scenes().get_current_program_scene().await?;
            println!("{}", scene.scene_name);
        }
        ScenesCommand::Set { name } => {
            client
                .scenes()
                .set_current_program_scene(&SetCurrentProgramScene::new().scene_name(name))
                .await?;
            println!("ok");
        }
    }
    Ok(())
}

async fn inputs(client: &Client, action: InputsCommand) -> Result<(), Box<dyn Error>> {
    let inputs = client.inputs();
    match action {
        InputsCommand::List => {
            let list = inputs.get_input_list(&GetInputList::new()).await?;
            for input in list.inputs {
                println!("{}  [{}]", input.input_name, input.input_kind);
            }
        }
        InputsCommand::Info { name } => {
            let mute = inputs
                .get_input_mute(&GetInputMute::new().input_name(&name))
                .await?;
            let volume = inputs
                .get_input_volume(&GetInputVolume::new().input_name(name))
                .await?;
            println!("muted: {}", mute.input_muted);
            println!(
                "volume: {:.3} mul ({:.1} dB)",
                volume.input_volume_mul, volume.input_volume_db
            );
        }
        InputsCommand::Mute { name } => {
            inputs
                .set_input_mute(&SetInputMute::new(true).input_name(name))
                .await?;
            println!("muted");
        }
        InputsCommand::Unmute { name } => {
            inputs
                .set_input_mute(&SetInputMute::new(false).input_name(name))
                .await?;
            println!("unmuted");
        }
        InputsCommand::Toggle { name } => {
            let response = inputs
                .toggle_input_mute(
                    &obs_websocket_core::requests::ToggleInputMute::new().input_name(name),
                )
                .await?;
            println!("muted: {}", response.input_muted);
        }
        InputsCommand::Volume { name, mul, db } => {
            if mul.is_none() && db.is_none() {
                let volume = inputs
                    .get_input_volume(&GetInputVolume::new().input_name(name))
                    .await?;
                println!(
                    "{:.3} mul ({:.1} dB)",
                    volume.input_volume_mul, volume.input_volume_db
                );
            } else {
                let mut request = SetInputVolume::new().input_name(name);
                if let Some(mul) = mul {
                    request = request.input_volume_mul(mul);
                }
                if let Some(db) = db {
                    request = request.input_volume_db(db);
                }
                inputs.set_input_volume(&request).await?;
                println!("ok");
            }
        }
    }
    Ok(())
}

async fn stream(client: &Client, action: StreamCommand) -> Result<(), Box<dyn Error>> {
    let stream = client.stream();
    match action {
        StreamCommand::Status => {
            let status = stream.get_stream_status().await?;
            println!(
                "{}  {}",
                output_line(status.output_active, &status.output_timecode),
                format_args!(
                    "congestion {:.2}  skipped {}/{}",
                    status.output_congestion,
                    status.output_skipped_frames,
                    status.output_total_frames
                )
            );
        }
        StreamCommand::Start => {
            stream.start_stream().await?;
            println!("started");
        }
        StreamCommand::Stop => {
            stream.stop_stream().await?;
            println!("stopped");
        }
        StreamCommand::Toggle => {
            let response = stream.toggle_stream().await?;
            println!("{}", on_off(response.output_active));
        }
    }
    Ok(())
}

async fn record(client: &Client, action: RecordCommand) -> Result<(), Box<dyn Error>> {
    let record = client.record();
    match action {
        RecordCommand::Status => {
            let status = record.get_record_status().await?;
            if status.output_paused {
                println!("paused  {}", status.output_timecode);
            } else {
                println!(
                    "{}",
                    output_line(status.output_active, &status.output_timecode)
                );
            }
        }
        RecordCommand::Start => {
            record.start_record().await?;
            println!("started");
        }
        RecordCommand::Stop => {
            record.stop_record().await?;
            println!("stopped");
        }
        RecordCommand::Toggle => {
            let response = record.toggle_record().await?;
            println!("{}", on_off(response.output_active));
        }
        RecordCommand::Pause => {
            record.toggle_record_pause().await?;
            println!("toggled pause");
        }
    }
    Ok(())
}

async fn virtual_cam(client: &Client, action: VirtualCamCommand) -> Result<(), Box<dyn Error>> {
    let outputs = client.outputs();
    match action {
        VirtualCamCommand::Status => {
            let status = outputs.get_virtual_cam_status().await?;
            println!("{}", on_off(status.output_active));
        }
        VirtualCamCommand::Start => {
            outputs.start_virtual_cam().await?;
            println!("started");
        }
        VirtualCamCommand::Stop => {
            outputs.stop_virtual_cam().await?;
            println!("stopped");
        }
        VirtualCamCommand::Toggle => {
            let response = outputs.toggle_virtual_cam().await?;
            println!("{}", on_off(response.output_active));
        }
    }
    Ok(())
}

async fn replay(client: &Client, action: ReplayCommand) -> Result<(), Box<dyn Error>> {
    let outputs = client.outputs();
    match action {
        ReplayCommand::Status => {
            let status = outputs.get_replay_buffer_status().await?;
            println!("{}", on_off(status.output_active));
        }
        ReplayCommand::Start => {
            outputs.start_replay_buffer().await?;
            println!("started");
        }
        ReplayCommand::Stop => {
            outputs.stop_replay_buffer().await?;
            println!("stopped");
        }
        ReplayCommand::Toggle => {
            let response = outputs.toggle_replay_buffer().await?;
            println!("{}", on_off(response.output_active));
        }
        ReplayCommand::Save => {
            outputs.save_replay_buffer().await?;
            println!("saved");
        }
    }
    Ok(())
}

async fn hotkeys(client: &Client, action: HotkeysCommand) -> Result<(), Box<dyn Error>> {
    match action {
        HotkeysCommand::List => {
            let list = client.general().get_hotkey_list().await?;
            for name in list.hotkeys {
                println!("{name}");
            }
        }
        HotkeysCommand::Trigger { name } => {
            client
                .general()
                .trigger_hotkey_by_name(&TriggerHotkeyByName::new(name))
                .await?;
            println!("ok");
        }
    }
    Ok(())
}

fn print_json(value: &impl serde::Serialize) -> Result<(), Box<dyn Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn on_off(active: bool) -> &'static str {
    if active { "on" } else { "off" }
}

fn output_line(active: bool, timecode: &str) -> String {
    if active {
        format!("on  {timecode}")
    } else {
        "off".to_string()
    }
}
