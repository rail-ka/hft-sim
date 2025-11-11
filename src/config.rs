use eyre::bail;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::read_to_string, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub scenario: String,
    pub duration_secs: u64,
    /// 4-8
    pub producers: Producers,
    /// 4-8
    pub processors: Processors,
    /// 2-4
    pub strategies: Strategies,
    pub stage1_rules: Vec<Stage1Rule>,
    pub stage2_rules: Vec<Stage2Rule>,
}

impl Config {
    pub fn init(path: impl AsRef<Path>) -> eyre::Result<Self> {
        let path = path.as_ref();
        info!("config_path: {}", path.to_string_lossy());
        let config_data = read_to_string(path)?;
        let config: Self = serde_json::from_str(&config_data)?;

        if config.strategies.count as usize != config.strategies.processing_times_ns.len() {
            bail!("strategies count error");
        }

        let fmt = human_format::Formatter::new();
        let messages_per_sec = fmt.format(config.producers.messages_per_sec as f64);
        info!("Loaded config for scenario: {}", config.scenario);
        info!(
            messages_per_sec,
            config.producers.count, config.processors.count, config.strategies.count
        );
        Ok(config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase {
    pub duration_ms: u64,
    pub multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Producers {
    pub count: u64,
    /// per producer
    pub messages_per_sec: u64,
    pub distribution: HashMap<String, f64>,
    pub burst_pattern: Option<Vec<Phase>>,
}

impl Producers {
    pub fn distribution_parse(&self) -> Vec<(u64, f64)> {
        self.distribution
            .iter()
            .map(|(k, v)| {
                let id: u64 = k.trim_start_matches("msg_type_").parse().unwrap();
                (id, *v)
            })
            .collect::<Vec<_>>()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Processors {
    pub count: u64,
    pub processing_times_ns: HashMap<String, u64>,
}

impl Processors {
    pub fn parse(&self) -> Vec<(u64, u64)> {
        let processors_processing_times = self
            .processing_times_ns
            .iter()
            .map(|(k, v)| {
                let id: u64 = k.trim_start_matches("msg_type_").parse().unwrap();
                (id, *v)
            })
            .sorted_unstable_by_key(|(id, _)| *id)
            .collect::<Vec<_>>();
        debug!(?processors_processing_times);
        processors_processing_times
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Strategies {
    pub count: u64,
    pub processing_times_ns: HashMap<String, u64>,
}

impl Strategies {
    pub fn parse(&self) -> Vec<(usize, (u64, u64))> {
        let strategies = self
            .processing_times_ns
            .iter()
            .map(|(k, v)| {
                let id: u64 = k.trim_start_matches("strategy_").parse().unwrap();
                (id, *v)
            })
            .sorted_unstable_by_key(|(id, _)| *id)
            .enumerate()
            .collect_vec();
        debug!(?strategies);
        strategies
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage1Rule {
    pub msg_type: u64,
    pub processors: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage2Rule {
    pub msg_type: u64,
    pub strategy: u64,
    pub ordering_required: bool,
}
