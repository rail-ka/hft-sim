use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use itertools::Itertools;
use quanta::Clock;
use rand::{rng, seq::SliceRandom};

use crate::{config::Phase, traits::ProducerSender, types::Message};

pub struct ProducerWorker<S: ProducerSender> {
    pub id: u64,
    pub duration_secs: u64,
    pub messages_per_sec: u64,
    pub distribution: Vec<(u64, f64)>,
    pub burst_pattern: Option<Vec<Phase>>,
    pub stage1: S,
    pub zero_messages: Arc<AtomicU64>,
    pub start_ts: u64,
    pub clock: Clock,
}

impl<S: ProducerSender> ProducerWorker<S> {
    pub fn run(self) {
        let Self {
            id,
            duration_secs,
            messages_per_sec,
            mut distribution,
            burst_pattern,
            mut stage1,
            zero_messages: zero_messages_counts,
            start_ts,
            clock,
        } = self;
        let fmt = human_format::Formatter::new();
        let mut msg = Message {
            producer_id: id,
            ty: 0,
            seq: 0,
            timestamp: 0,
        };

        distribution.sort_unstable_by_key(|(k, _)| *k);
        let distribution_pattern = create_distribution_pattern(&distribution);
        info!("distribution_pattern: {distribution_pattern:?}");
        let mut distribution_pattern = distribution_pattern.into_iter().cycle();

        let mut err_arr = [0u64; 8];

        let mut total_msg = 0usize;
        let mut total_zero_msgs = 0u64;
        // let mut wait_cycles = 0u64;
        let mut iter = 0u64;

        let mut closure = |duration_ms: u64, msg_per_ms: u64, nano_per_msg: u64| {
            for ms in 0..duration_ms {
                let start_of_ms_raw = clock.raw();

                for i in 0..msg_per_ms {
                    let target_nanos = (i + 1) * nano_per_msg;
                    let mut delta = clock.delta_as_nanos(start_of_ms_raw, clock.raw());
                    while delta < target_nanos {
                        delta = clock.delta_as_nanos(start_of_ms_raw, clock.raw());
                        // wait_cycles += 1;
                    }

                    let msg_type = distribution_pattern.next().unwrap();
                    msg.seq += 1;
                    msg.ty = msg_type;
                    msg.timestamp = clock.delta_as_nanos(start_ts, clock.raw());
                    let res = stage1.send(msg);
                    if !res {
                        err_arr[msg_type as usize] += 1;
                    } else {
                        total_msg += 1;
                        if msg_type == 0 {
                            total_zero_msgs += 1;
                        }
                    }
                }
                if ms % 1000 == 0 {
                    let total_msg = fmt.format(total_msg as f64);
                    // let wait_cycles_s = fmt.format(wait_cycles as f64);
                    // wait_cycles = 0;
                    iter += 1;
                    info!(id, iter, total_msg);
                }
            }
        };

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

                'a: loop {
                    for (duration_ms, msg_per_ms, nano_per_msg) in pattern.iter().copied() {
                        closure(duration_ms, msg_per_ms, nano_per_msg);
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
                let duration_ms = duration_secs * 1000;
                let msg_per_ms = messages_per_sec / 1000;
                let nano_per_msg = 1_000_000_000u64 / messages_per_sec;
                info!(nano_per_msg);
                closure(duration_ms, msg_per_ms, nano_per_msg);
            }
        }

        for (ty, count) in err_arr.iter().enumerate() {
            if *count != 0 {
                error!(ty, "{}", fmt.format(*count as f64));
            }
        }
        zero_messages_counts.fetch_add(total_zero_msgs, Ordering::SeqCst);
        let total_msg = fmt.format(total_msg as f64);
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
