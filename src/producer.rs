use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crossbeam_channel::Sender;
use itertools::Itertools;
use quanta::Clock;
use rand::{rng, seq::SliceRandom};

use crate::{
    config::{Phase, Stage1Rule},
    types::Message,
    utils::timestamp,
};

pub struct ProducerWorker {
    pub id: u64,
    pub duration_secs: u64,
    pub messages_per_sec: u64,
    pub distribution: Vec<(u64, f64)>,
    pub burst_pattern: Option<Vec<Phase>>,
    pub processors: Arc<Vec<Sender<Message>>>,
    pub stage1_rules: Vec<Stage1Rule>,
    pub zero_messages_counts: Arc<AtomicU64>,
}

impl ProducerWorker {
    pub fn run(self) {
        let Self {
            id,
            duration_secs,
            messages_per_sec,
            mut distribution,
            burst_pattern,
            processors,
            stage1_rules,
            zero_messages_counts,
        } = self;
        let mut ts = timestamp();
        let clock = Clock::new();
        let mut msg = Message {
            producer_id: id,
            ty: 0,
            seq: 0,
            timestamp: 0,
        };

        distribution.sort_unstable_by_key(|(k, _)| *k);
        let distribution_pattern = create_distribution_pattern(&distribution);
        info!("distribution_pattern: {distribution_pattern:?}");
        let pattern_len = distribution_pattern.len();
        if pattern_len == 0 {
            info!(
                "Error: Distribution pattern is empty. Producer {} exiting.",
                id
            );
            return;
        }

        let mut err_arr = [[0u64; 8]; 8];

        let mut total_msg = 0usize;
        let mut total_zero_msgs = 0u64;

        match burst_pattern {
            Some(pattern) => {
                let mut duration_mss = duration_secs * 1000;

                let pattern = pattern
                    .into_iter()
                    .map(|phase| {
                        let msg_per_ms =
                            ((messages_per_sec as f64 * phase.multiplier) / 1000f64).round() as u64;
                        let nano_per_msg = 1_000_000u64 / msg_per_ms;
                        (phase.duration_ms, msg_per_ms, nano_per_msg)
                    })
                    .collect_vec();
                info!(?pattern);

                let mut prev = clock.raw();

                'a: loop {
                    for (duration_ms, msg_per_ms, nano_per_msg) in pattern.iter().copied() {
                        for _ in 0..duration_ms {
                            for i in 0..msg_per_ms {
                                let mut raw = clock.raw();
                                let mut delta = clock.delta_as_nanos(prev, raw);
                                while delta < nano_per_msg {
                                    std::hint::spin_loop();
                                    raw = clock.raw();
                                    delta = clock.delta_as_nanos(prev, raw);
                                }
                                prev = raw;
                                ts += delta;

                                msg.seq += 1;
                                msg.ty = distribution_pattern[(i as usize) % pattern_len];
                                let processor_id: u64 = stage1_rules
                                    .iter()
                                    .find(|i| i.msg_type == msg.ty)
                                    .unwrap()
                                    .processors[0];
                                let res = processors[processor_id as usize].try_send(msg);
                                if res.is_err() {
                                    err_arr[msg.ty as usize][processor_id as usize] += 1;
                                } else {
                                    total_msg += 1;
                                    if msg.ty == 0 {
                                        total_zero_msgs += 1;
                                    }
                                }
                            }
                        }
                        let (n, overflow) = duration_mss.overflowing_sub(duration_ms);
                        if overflow {
                            info!(duration_mss, duration_ms, "time end");
                            break 'a;
                        }
                        duration_mss = n;
                    }
                }
            }
            None => {
                let nano_per_msg = 1_000_000_000u64 / messages_per_sec;
                info!(nano_per_msg);

                for sec in 0..duration_secs {
                    info!(id, sec);

                    let mut prev = clock.raw();

                    for i in 0..messages_per_sec {
                        let mut raw = clock.raw();
                        let mut delta = clock.delta_as_nanos(prev, raw);
                        while delta < nano_per_msg {
                            std::hint::spin_loop();
                            raw = clock.raw();
                            delta = clock.delta_as_nanos(prev, raw);
                        }
                        prev = raw;
                        ts += delta;

                        let msg_type = distribution_pattern[(i as usize) % pattern_len];
                        msg.seq += 1;
                        msg.timestamp = ts;
                        msg.ty = msg_type;
                        let processor_id: u64 = stage1_rules
                            .iter()
                            .find(|i| i.msg_type == msg_type)
                            .unwrap()
                            .processors[0];
                        let res = processors[processor_id as usize].try_send(msg);
                        if res.is_err() {
                            err_arr[msg_type as usize][processor_id as usize] += 1;
                        } else {
                            total_msg += 1;
                            if msg_type == 0 {
                                total_zero_msgs += 1;
                            }
                        }
                    }
                }
            }
        }

        let fmt = human_format::Formatter::new();
        for (ty, inner) in err_arr.iter().enumerate() {
            for (processor, count) in inner.iter().enumerate() {
                if *count != 0 {
                    error!(ty, processor, "{}", fmt.format(*count as f64));
                }
            }
        }
        zero_messages_counts.fetch_add(total_zero_msgs, Ordering::SeqCst);
        info!(id, total_msg);
    }
}

fn create_distribution_pattern(distribution: &[(u64, f64)]) -> Vec<u64> {
    const PATTERN_SIZE: usize = 100;
    let mut pattern = Vec::with_capacity(PATTERN_SIZE);

    let mut items = distribution
        .iter()
        .map(|(msg_type, fraction)| {
            let count = (PATTERN_SIZE as f64 * fraction).round() as usize;
            std::iter::repeat_n(*msg_type, count)
        })
        .collect::<Vec<_>>();

    debug!(?items);

    loop {
        let mut added_in_this_round = false;

        for item_iter in items.iter_mut() {
            if let Some(value) = item_iter.next() {
                pattern.push(value);
                added_in_this_round = true;
            }
        }

        if !added_in_this_round {
            break;
        }
    }

    pattern.truncate(PATTERN_SIZE);
    let mut rng = rng();
    pattern.shuffle(&mut rng);
    pattern
}
