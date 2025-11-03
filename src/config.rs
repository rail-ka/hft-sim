use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase {
    pub duration_ms: u64,
    pub multiplier: f32,
    pub messages_per_sec: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BurstPattern {
    pub phases: Vec<Phase>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Producers {
    pub count: u32,
    /// per producer
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
