use crate::{config::Stage2Rule, types::HandledMessage};

pub struct Stage2 {
    pub stage2_rules: Vec<Stage2Rule>,
}

impl Stage2 {
    pub fn send(&self, msg: HandledMessage) {}
}
