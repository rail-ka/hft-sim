use std::{fs::File, path::PathBuf};

use arrayvec::ArrayVec;

use crate::state::State;

mod config;
mod log;
mod processor;
mod producer;
mod queue;
mod queue_router;
mod router;
mod router_queue;
mod state;
mod strategy;
mod traits;
mod types;
mod utils;

#[macro_use]
extern crate tracing;

pub type Arr<T> = ArrayVec<T, 8>;

#[derive(argh::FromArgs)]
/// Args
struct Args {
    /// config path
    #[argh(positional)]
    config: PathBuf,

    /// mode: queue, router
    #[argh(option, short = 'm', default = "Mode::Router")]
    mode: Mode,
}

#[derive(strum::EnumString)]
#[strum(serialize_all = "kebab-case")]
enum Mode {
    Queue,
    Router,
    RouterQueue,
    QueueRouter,
}

fn main() -> eyre::Result<()> {
    log::init();
    color_eyre::install()?;

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "pthread"])
        .build()
        .unwrap();

    let args: Args = argh::from_env();

    let config = config::Config::init(args.config.canonicalize()?)?;

    let state = State::new();

    match args.mode {
        Mode::Queue => queue::run(config, state)?,
        Mode::Router => router::run(config, state)?,
        Mode::RouterQueue => router_queue::run(config, state)?,
        Mode::QueueRouter => queue_router::run(config, state)?,
    }

    if let Ok(report) = guard.report().build() {
        let file = File::create("flamegraph.svg").unwrap();
        report.flamegraph(file).unwrap();
    };
    Ok(())
}
