use std::sync::{Arc, atomic::AtomicU64};

use crate::config::Config;

pub fn run(config: Config) -> eyre::Result<()> {
    let Config {
        scenario: _,
        duration_secs,
        producers: c_producers,
        processors: c_processors,
        strategies: c_strategies,
        stage1_rules,
        stage2_rules,
    } = config;

    let zero_messages_counts = Arc::new(AtomicU64::new(0));
    let handled_zero_messages_counts = Arc::new(AtomicU64::new(0));

    // let mut producers_handles = Vec::with_capacity(c_producers.count as usize);
    // let mut processors_handles = Vec::with_capacity(c_processors.count as usize);
    // let mut strategies_handles = Vec::with_capacity(c_strategies.count as usize);

    let mut core_ids = core_affinity::get_core_ids().unwrap();
    debug!(?core_ids);
    core_ids.reverse();

    todo!()
}
