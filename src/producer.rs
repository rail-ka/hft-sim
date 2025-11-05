use std::{sync::Arc, thread, time::Duration};

use quanta::{Clock, IntoNanoseconds};

use crate::{Stage1, types::Message};

pub struct ProducerWorker {
    pub id: u64,
    pub duration_secs: u64,
    pub messages_per_sec: u32,
    pub distribution: Vec<(u64, f64)>,
    pub stage: Arc<Stage1>,
}

fn timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos() as u64
}

impl ProducerWorker {
    pub fn run(self) {
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
        info!("ts: {ts}, instant: {instant:?}");
        let mut msg = Message {
            ty: 0,
            producer_id: id,
            seq: 0,
            timestamp: 0,
        };
        let nano_per_msg: u64 = 1_000_000_000u64 / (messages_per_sec as u64);
        info!("nano_per_msg: {nano_per_msg}");

        distribution.sort_unstable_by_key(|(k, _)| *k);
        let distribution_pattern = create_distribution_pattern(&distribution, 100);
        info!("distribution_pattern: {distribution_pattern:?}");
        let pattern_len = distribution_pattern.len();
        if pattern_len == 0 {
            info!(
                "Error: Distribution pattern is empty. Producer {} exiting.",
                id
            );
            return;
        }

        for s in 0..duration_secs {
            info!("sec: {s}");
            let elapsed = instant.elapsed().into_nanos();
            let now = ts + elapsed;

            for i in 0..(messages_per_sec as usize) {
                let target_time = now + (nano_per_msg * i as u64);
                let elapsed = instant.elapsed().into_nanos();
                let now = ts + elapsed;
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
