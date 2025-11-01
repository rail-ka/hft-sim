use std::{env, fs, thread};

mod config;
mod types;
use crossbeam_queue::ArrayQueue;
use quanta::Clock;

use crate::{config::Config, types::Message};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <config_path>", args[0]);
        std::process::exit(1);
    }
    let config_path = &args[1];

    let config_data = fs::read_to_string(config_path).unwrap_or_else(|err| {
        eprintln!("Failed to read config file '{}': {}", config_path, err);
        std::process::exit(1);
    });

    let config: config::Config = serde_json::from_str(&config_data).unwrap_or_else(|err| {
        eprintln!("Failed to parse config: {}", err);
        std::process::exit(1);
    });

    let Config {
        scenario,
        duration_secs,
        producers: c_producers,
        processors: c_processors,
        strategies: c_strategies,
        stage1_rules,
        stage2_rules,
    } = config;

    println!("Loaded config for scenario: {}", scenario);

    if c_producers.count as usize != c_processors.processing_times_ns.len() {
        panic!("producers count error");
    }

    let mut producers = Vec::with_capacity(c_producers.count as usize);
    let mut processors = Vec::with_capacity(c_processors.count as usize);
    let mut strategies = Vec::with_capacity(c_strategies.count as usize);

    for (k, v) in c_producers.distribution {
        let id: u64 = k.trim_start_matches("msg_type_").parse().unwrap();
        let stage1_rules = stage1_rules.clone();
        let handle = thread::spawn(move || {
            let clock = Clock::new();
            let mut seq = 0;
            let msg = Message {
                ty: 0,
                producer_id: id,
                seq,
                timestamp: clock.raw(),
            };
        });
        producers.push(handle);
    }

    for (k, v) in c_processors.processing_times_ns {
        let id: u32 = k.trim_start_matches("msg_type_").parse().unwrap();
        let handle = thread::spawn(move || {});
        processors.push(handle);
    }

    for (k, v) in c_strategies.processing_times_ns {
        let id: u32 = k.trim_start_matches("strategy_").parse().unwrap();
        let handle = thread::spawn(move || {});
        strategies.push(handle);
    }
}
