use std::{fs::File, path::PathBuf};

mod config;
mod lmax;
mod log;
mod processor;
mod producer;
mod queue;
mod router;
mod stage1;
mod stage2;
mod strategy;
mod types;
mod utils;

#[macro_use]
extern crate tracing;

#[derive(argh::FromArgs)]
/// Args
struct Args {
    /// how high to go
    #[argh(option)]
    config: PathBuf,

    /// mode: queue, router, lmax
    #[argh(option, default = "Mode::Router")]
    mode: Mode,
}

#[derive(strum::EnumString)]
enum Mode {
    Queue,
    Router,
    Lmax,
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
    info!("Loaded config for scenario: {}", config.scenario);

    match args.mode {
        Mode::Queue => queue::run(config)?,
        Mode::Router => router::run(config)?,
        Mode::Lmax => lmax::run(config)?,
    }

    if let Ok(report) = guard.report().build() {
        let file = File::create("flamegraph.svg").unwrap();
        report.flamegraph(file).unwrap();
    };
    Ok(())
}
