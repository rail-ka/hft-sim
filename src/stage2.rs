use crate::{
    config::Stage2Rule,
    types::{HandledMessage, Message},
};

pub struct Stage2 {
    pub stage2_rules: Vec<Stage2Rule>,
}

impl Stage2 {
    fn recv(&self) -> Option<Message> {
        todo!()
    }
    fn send(&self, msg: HandledMessage) {}
}
