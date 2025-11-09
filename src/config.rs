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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Processors {
    pub count: u64,
    pub processing_times_ns: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Strategies {
    pub count: u64,
    pub processing_times_ns: HashMap<String, u64>,
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
