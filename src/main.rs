#![allow(dead_code, unused_variables)]
use std::{env, fs, path::PathBuf, sync::Arc, thread};

mod config;
mod log;
mod processor;
mod producer;
mod strategy;
mod types;

#[macro_use]
extern crate tracing;

use crossbeam_queue::ArrayQueue;

use crate::{
    config::Config,
    producer::ProducerWorker,
    types::{HandledMesage, Message},
};

fn main() {
    log::init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        error!("Usage: {} <config_path>", args[0]);
        std::process::exit(1);
    }
    let config_path = &args[1];
    let config_path = PathBuf::from(config_path.as_str()).canonicalize().unwrap();

    let config_data = fs::read_to_string(&config_path).unwrap_or_else(|err| {
        error!("Failed to read config file {:?}: {}", config_path, err);
        std::process::exit(1);
    });

    let config: config::Config = serde_json::from_str(&config_data).unwrap_or_else(|err| {
        error!("Failed to parse config: {}", err);
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

    info!("Loaded config for scenario: {}", scenario);

    if c_processors.count as usize != c_processors.processing_times_ns.len() {
        panic!("processors count error");
    }
    if c_strategies.count as usize != c_strategies.processing_times_ns.len() {
        panic!("strategies count error");
    }

    let mut producers = Vec::with_capacity(c_producers.count as usize);
    let mut processors = Vec::with_capacity(c_processors.count as usize);
    let mut strategies = Vec::with_capacity(c_strategies.count as usize);

    let mut stage1_queues = stage1_rules
        .into_iter()
        .map(|rule| {
            rule.msg_type;
            rule.processors;
            let queue = ArrayQueue::<Message>::new(512 * 512);
            queue
        })
        .collect::<Vec<_>>();
    let mut stage2_queues = stage2_rules
        .into_iter()
        .map(|rule| {
            rule.strategy;
            rule.msg_type;
            rule.ordering_required;
            let queue = ArrayQueue::<HandledMesage>::new(512 * 512);
            queue
        })
        .collect::<Vec<_>>();

    let stage1 = Stage1 {};
    let stage1 = Arc::new(stage1);

    let distribution = c_producers
        .distribution
        .into_iter()
        .map(|(k, v)| {
            let id: u64 = k.trim_start_matches("msg_type_").parse().unwrap();
            (id, v)
        })
        .collect::<Vec<_>>();

    for i in 0..c_producers.count {
        let stage = stage1.clone();
        let distribution = distribution.clone();
        let worker = ProducerWorker {
            id: i as u64,
            duration_secs,
            messages_per_sec: c_producers.messages_per_sec.unwrap(),
            distribution,
            stage,
        };
        let handle = thread::spawn(|| worker.run());
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

    for j in producers {
        j.join().unwrap();
    }
}

pub struct Stage1 {}

impl Stage1 {
    pub fn send(&self, msg: Message) {}
}

pub struct Stage2 {}

impl Stage2 {
    pub fn senf(&self, msg: HandledMesage) {}
}
