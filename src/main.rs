use std::{fs::File, path::PathBuf};

use arrayvec::ArrayVec;

mod config;
mod log;
mod processor;
mod producer;
mod queue;
mod router;
mod stage1;
mod stage2;
mod strategy;
mod traits;
mod types;
// mod utils;

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
#[strum(serialize_all = "lowercase")]
enum Mode {
    Queue,
    Router,
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

    match args.mode {
        Mode::Queue => queue::run(config)?,
        Mode::Router => router::run(config)?,
    }

    if let Ok(report) = guard.report().build() {
        let file = File::create("flamegraph.svg").unwrap();
        report.flamegraph(file).unwrap();
    };
    Ok(())
}
