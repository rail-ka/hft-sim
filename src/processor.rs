use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};
use quanta::Clock;

use crate::{
    config::Stage2Rule,
    types::{HandledMesage, Message},
    utils::timestamp,
};

// pub struct ProcessorIdQueue {
//     pub id: u64,
//     pub queue: ProcessorQueue,
// }

// pub type ProcessorQueue = ArrayQueue<Message>;
pub type ProcessorReceiver = Receiver<Message>;
pub type SrategySender = Sender<HandledMesage>;

pub struct ProcessorWorker {
    pub id: u64,
    pub processing_times: Vec<(u64, u64)>,
    pub strategies: Arc<Vec<SrategySender>>,
    pub receiver: ProcessorReceiver,
    pub stage2_rules: Vec<Stage2Rule>,
}

impl ProcessorWorker {
    pub fn run(self) {
        let ProcessorWorker {
            id,
            processing_times,
            strategies,
            receiver,
            stage2_rules,
        } = self;
        let clock = Clock::new();
        while let Ok(msg) = receiver.recv() {
            let strategy = stage2_rules
                .iter()
                .find(|i| i.msg_type == msg.ty)
                .unwrap()
                .strategy;
            let strategy = &strategies[strategy as usize];
            let now = timestamp();
            let res = strategy.try_send(HandledMesage {
                msg,
                processor_id: id,
                processing_ts: now,
            });
            if let Err(err) = res {
                error!("send error for msg: {msg:?}");
            }
        }
    }
}
