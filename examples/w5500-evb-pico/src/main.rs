//! OBS WebSocket client for the WIZnet W5500-EVB-Pico.
//!
//! GPIO16–21 are the on-board W5500 SPI bus. GPIO25 is the user LED, which
//! turns on after OBS identifies this client.
//!
//! ```text
//! cargo check -p w5500-evb-pico --target thumbv6m-none-eabi
//! ```
//!
//! `OBS_WS_HOST` defaults to `192.168.1.10` and must be an IPv4 address.
//! `OBS_WS_PORT` defaults to `4455`. `OBS_WS_PASSWORD` defaults to none.
//! These are compile-time environment variables (`option_env`).

#![no_std]
#![no_main]

extern crate alloc;

use core::fmt::{Debug, Display};
use core::mem::MaybeUninit;
use core::str::FromStr;

use alloc::format;

use defmt::*;
use defmt_rtt as _;
use embassy_executor::Spawner;
use embassy_net::Stack;
use embassy_net_wiznet::chip::W5500;
use embassy_net_wiznet::*;
use embassy_rp::clocks::RoscRng;
use embassy_rp::gpio::{Input, Level, Output, Pull};
use embassy_rp::peripherals::{DMA_CH0, DMA_CH1, SPI0};
use embassy_rp::spi::{Async, Config as SpiConfig, Spi};
use embassy_rp::{bind_interrupts, dma};
use embassy_time::{Delay, Duration, Instant, Timer};
use embedded_alloc::LlffHeap;
use embedded_hal_bus::spi::ExclusiveDevice;
use embedded_io_async_06::{ErrorType, Read, Write};
use embedded_io_async_07::Read as Read07;
use embedded_io_async_07::Write as Write07;
use obs_websocket_core::SessionConfig;
use obs_websocket_core::requests::GetVersion;
use obs_websocket_embassy::EmbassyTransport;
use obs_websocket_io::{Connection, Timer as ObsTimer};
use panic_probe as _;
use static_cell::StaticCell;

#[global_allocator]
static HEAP: LlffHeap = LlffHeap::empty();

bind_interrupts!(struct Irqs {
    DMA_IRQ_0 => dma::InterruptHandler<DMA_CH0>, dma::InterruptHandler<DMA_CH1>;
});

type SpiBus = Spi<'static, SPI0, Async>;
type W5500Device = ExclusiveDevice<SpiBus, Output<'static>, Delay>;

#[embassy_executor::task]
async fn ethernet_task(
    runner: Runner<'static, W5500, W5500Device, Input<'static>, Output<'static>>,
) -> ! {
    runner.run().await
}

#[embassy_executor::task]
async fn net_task(mut runner: embassy_net::Runner<'static, Device<'static>>) -> ! {
    runner.run().await
}

#[embassy_executor::main]
async fn main(spawner: Spawner) -> ! {
    init_heap();
    let p = embassy_rp::init(Default::default());
    let mut rng = RoscRng;
    let mut led = Output::new(p.PIN_25, Level::Low);

    let mut spi_cfg = SpiConfig::default();
    spi_cfg.frequency = 50_000_000;
    let (miso, mosi, clk) = (p.PIN_16, p.PIN_19, p.PIN_18);
    let spi = Spi::new(p.SPI0, clk, mosi, miso, p.DMA_CH0, p.DMA_CH1, Irqs, spi_cfg);
    let cs = Output::new(p.PIN_17, Level::High);
    let w5500_int = Input::new(p.PIN_21, Pull::Up);
    let w5500_reset = Output::new(p.PIN_20, Level::High);

    let mac_addr = [0x02, 0x00, 0x00, 0x00, 0x00, 0x00];
    static STATE: StaticCell<State<8, 8>> = StaticCell::new();
    let state = STATE.init(State::<8, 8>::new());
    let (device, runner) = embassy_net_wiznet::new(
        mac_addr,
        state,
        ExclusiveDevice::new(spi, cs, Delay),
        w5500_int,
        w5500_reset,
    )
    .await
    .unwrap();
    spawner.spawn(unwrap!(ethernet_task(runner)));

    static RESOURCES: StaticCell<embassy_net::StackResources<3>> = StaticCell::new();
    let (stack, runner) = embassy_net::new(
        device,
        embassy_net::Config::dhcpv4(Default::default()),
        RESOURCES.init(embassy_net::StackResources::new()),
        rng.next_u64(),
    );
    spawner.spawn(unwrap!(net_task(runner)));

    info!("Waiting for DHCP...");
    let config = wait_for_config(stack).await;
    info!("IP address: {:?}", config.address.address());

    let timer = Clock {
        origin: Instant::now(),
    };
    loop {
        led.set_low();
        if connect_obs(stack, &timer, &mut led).await.is_err() {
            Timer::after_secs(2).await;
        }
    }
}

async fn connect_obs(
    stack: Stack<'static>,
    timer: &Clock,
    led: &mut Output<'static>,
) -> Result<(), ()> {
    let mut rx_buffer = [0u8; 4096];
    let mut tx_buffer = [0u8; 4096];
    let mut socket = embassy_net::tcp::TcpSocket::new(stack, &mut rx_buffer, &mut tx_buffer);
    socket.set_timeout(Some(Duration::from_secs(10)));

    let host = obs_host();
    let port = obs_port();
    let address = match embassy_net::Ipv4Address::from_str(host) {
        Ok(address) => address,
        Err(_) => {
            warn!("OBS_WS_HOST must be an IPv4 address");
            return Err(());
        }
    };
    info!("Connecting to {:?}:{}", address, port);
    if let Err(error) = socket.connect((address, port)).await {
        warn!("connect error: {:?}", Debug2Format(&error));
        return Err(());
    }

    let header = format!("{host}:{port}");
    let transport =
        match EmbassyTransport::<_, 4096, 4096>::connect(IoCompat { socket }, &header, 1).await {
            Ok(transport) => transport,
            Err(error) => {
                warn!("handshake error: {:?}", Debug2Format(&error));
                return Err(());
            }
        };
    let mut connection = match Connection::connect(
        transport,
        SessionConfig::default().password(obs_password()),
        2_000,
        timer,
    )
    .await
    {
        Ok(connection) => connection,
        Err(error) => {
            warn!("identify error: {:?}", Debug2Format(&error));
            return Err(());
        }
    };
    led.set_high();
    let version = match connection.request(&GetVersion::new(), timer).await {
        Ok(version) => version,
        Err(error) => {
            warn!("GetVersion error: {:?}", Debug2Format(&error));
            return Err(());
        }
    };
    info!(
        "obs-websocket {=str}",
        version.obs_web_socket_version.as_str()
    );
    loop {
        Timer::after_secs(60).await;
    }
}

async fn wait_for_config(stack: Stack<'static>) -> embassy_net::StaticConfigV4 {
    loop {
        if let Some(config) = stack.config_v4() {
            return config.clone();
        }
        Timer::after_millis(50).await;
    }
}

fn obs_host() -> &'static str {
    match option_env!("OBS_WS_HOST") {
        Some(host) if !host.is_empty() => host,
        _ => "192.168.1.10",
    }
}

fn obs_port() -> u16 {
    let raw = option_env!("OBS_WS_PORT").unwrap_or("4455");
    raw.parse().expect("OBS_WS_PORT is a u16")
}

fn obs_password() -> Option<&'static str> {
    match option_env!("OBS_WS_PASSWORD") {
        Some(password) if !password.is_empty() => Some(password),
        _ => None,
    }
}

fn init_heap() {
    const HEAP_SIZE: usize = 32 * 1024;
    static mut HEAP_MEM: [MaybeUninit<u8>; HEAP_SIZE] = [MaybeUninit::uninit(); HEAP_SIZE];
    unsafe {
        HEAP.init(core::ptr::addr_of_mut!(HEAP_MEM) as usize, HEAP_SIZE);
    }
}

struct Clock {
    origin: Instant,
}

impl ObsTimer for Clock {
    fn now_ms(&self) -> u64 {
        self.origin.elapsed().as_millis()
    }

    async fn wait(&self, duration_ms: u64) {
        Timer::after_millis(duration_ms).await;
    }
}

/// Adapts embassy-net's `embedded-io-async` 0.7 socket to the 0.6 traits used
/// by [`EmbassyTransport`].
struct IoCompat<S> {
    socket: S,
}

struct IoError<E>(E);

impl<E: Debug + Display> Debug for IoError<E> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        Debug::fmt(&self.0, formatter)
    }
}

impl<E: Debug + Display> Display for IoError<E> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

impl<E: Debug + Display> embedded_io_async_06::Error for IoError<E> {
    fn kind(&self) -> embedded_io_async_06::ErrorKind {
        embedded_io_async_06::ErrorKind::Other
    }
}

impl<S> ErrorType for IoCompat<S>
where
    S: embedded_io_async_07::ErrorType,
    <S as embedded_io_async_07::ErrorType>::Error: Debug + Display,
{
    type Error = IoError<<S as embedded_io_async_07::ErrorType>::Error>;
}

impl<S> Read for IoCompat<S>
where
    S: Read07,
    <S as embedded_io_async_07::ErrorType>::Error: Debug + Display,
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        Read07::read(&mut self.socket, buf).await.map_err(IoError)
    }
}

impl<S> Write for IoCompat<S>
where
    S: Write07,
    <S as embedded_io_async_07::ErrorType>::Error: Debug + Display,
{
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        Write07::write(&mut self.socket, buf).await.map_err(IoError)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Write07::flush(&mut self.socket).await.map_err(IoError)
    }
}
