use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub scenario: String,
    pub duration_secs: u64,
    pub producers: Producers,
    pub processors: Processors,
    pub strategies: Strategies,
    pub stage1_rules: Vec<Stage1Rule>,
    pub stage2_rules: Vec<Stage2Rule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase {
    pub duration_ms: u64,
    pub messages_per_sec: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BurstPattern {
    pub interval_ms: u64,
    pub phases: Vec<Phase>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Producers {
    pub count: u32,
    pub messages_per_sec: Option<u32>,
    pub burst_pattern: Option<BurstPattern>,
    pub distribution: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Processors {
    pub count: u32,
    pub processing_times_ns: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Strategies {
    pub count: u32,
    pub processing_times_ns: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage1Rule {
    pub msg_type: u32,
    pub processors: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage2Rule {
    pub msg_type: u32,
    pub strategy: u32,
    pub ordering_required: bool,
}
