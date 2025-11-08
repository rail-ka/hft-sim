use crate::{config::Stage1Rule, types::Message};

pub struct Stage1 {
    pub rules: Vec<Stage1Rule>,
}

impl Stage1 {
    pub fn send(&self, msg: Message) {}
}
