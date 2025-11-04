use std::{collections::HashMap, env, fs, path::PathBuf, sync::Arc, thread, time::Duration};

mod config;
mod types;
use crossbeam_queue::ArrayQueue;
use quanta::{Clock, Instant};

use crate::{
    config::Config,
    types::{HandledMesage, Message},
};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <config_path>", args[0]);
        std::process::exit(1);
    }
    let config_path = &args[1];
    let config_path = PathBuf::from(config_path.as_str()).canonicalize().unwrap();

    let config_data = fs::read_to_string(&config_path).unwrap_or_else(|err| {
        eprintln!("Failed to read config file {:?}: {}", config_path, err);
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

pub struct ProducerWorker {
    id: u64,
    duration_secs: u64,
    messages_per_sec: u32,
    distribution: Vec<(u64, f64)>,
    stage: Arc<Stage1>,
}

fn timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos() as u64
}

impl ProducerWorker {
    fn run(self) {
        let Self {
            id,
            duration_secs,
            messages_per_sec,
            mut distribution,
            stage,
        } = self;
        let ts = timestamp();
        let clock = Clock::new();
        let instant = clock.now();
        // let duration = instant.duration_since(std::time::UNIX_EPOCH);
        println!("ts: {ts}");
        let mut msg = Message {
            ty: 0,
            producer_id: id,
            seq: 0,
            timestamp: ts,
        };
        let nano_per_msg: u64 = 1_000_000_000u64 / (messages_per_sec as u64);
        println!("nano_per_msg: {nano_per_msg}");

        distribution.sort_unstable_by_key(|(k, _)| *k);
        let distribution_pattern = create_distribution_pattern(&distribution, 100);
        println!("distribution_pattern: {distribution_pattern:?}");
        let pattern_len = distribution_pattern.len();
        if pattern_len == 0 {
            println!(
                "Error: Distribution pattern is empty. Producer {} exiting.",
                id
            );
            return;
        }

        for s in 0..duration_secs {
            println!("sec: {s}");
            let raw_time = clock.raw();
            let start_time = clock.now();
            println!("raw_time: {raw_time}, start_time: {start_time:?}");

            for i in 0..(messages_per_sec as usize) {
                let target_time = raw_time + nano_per_msg;
                let now = clock.raw();
                let msg_type = distribution_pattern[i % pattern_len];
                msg.seq += 1;
                msg.timestamp = now;
                msg.ty = msg_type;
                stage.send(msg);
                if now < target_time {
                    let ns = target_time - now;
                    thread::sleep(Duration::from_nanos(ns));
                }
            }
            // let now = clock.raw();
            // let rem = now - raw_time;
        }
    }
}

fn create_distribution_pattern(distribution: &[(u64, f64)], pattern_size: usize) -> Vec<u64> {
    let mut pattern = Vec::with_capacity(pattern_size);

    for (msg_type, fraction) in distribution {
        let count = (pattern_size as f64 * fraction).round() as usize;

        for _ in 0..count {
            if pattern.len() < pattern_size {
                pattern.push(*msg_type);
            }
        }
    }

    // Из-за ошибок округления может не хватать/быть лишних элементов. Корректируем.
    // Добавляем самый вероятный тип, чтобы минимизировать искажение
    while pattern.len() < pattern_size {
        let default_type = distribution
            .iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .map(|(k, _)| *k)
            .unwrap_or(0);
        pattern.push(default_type);
    }

    pattern.truncate(pattern_size);
    pattern
}

pub struct Stage1 {}

impl Stage1 {
    pub fn send(&self, msg: Message) {}
}

pub struct Stage2 {}

impl Stage2 {
    pub fn senf(&self, msg: HandledMesage) {}
}
