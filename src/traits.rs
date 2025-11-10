use crossbeam_channel::{Receiver, Sender};

use crate::{Arr, config::Stage2Rule, types::HandledMessage};

pub trait StrategyReceiver {
    fn next(&mut self) -> Option<HandledMessage>;
    fn len(&self) -> usize;
}

impl StrategyReceiver for Receiver<HandledMessage> {
    fn next(&mut self) -> Option<HandledMessage> {
        self.recv().ok()
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl StrategyReceiver for rtrb::Consumer<HandledMessage> {
    fn next(&mut self) -> Option<HandledMessage> {
        if self.is_abandoned() {
            info!("StrategyReceiver stopped");
            return None;
        }
        let mut count = 0;
        loop {
            if count > 1000 && self.is_abandoned() {
                info!("StrategyReceiver stopped");
                return None;
            }
            count += 1;
            if !self.is_empty() {
                break;
            }
        }
        let msg = self.pop().unwrap();
        Some(msg)
    }

    fn len(&self) -> usize {
        self.slots()
    }
}

pub trait ProcessorSender {
    fn send(&mut self, msg: HandledMessage) -> bool;
}

pub struct QueueProcessorSender {
    pub queues: Arr<Sender<HandledMessage>>,
    pub rules: Vec<Stage2Rule>,
}

impl ProcessorSender for QueueProcessorSender {
    fn send(&mut self, msg: HandledMessage) -> bool {
        let strategy = self
            .rules
            .iter()
            .find(|i| i.msg_type == msg.msg.ty)
            .unwrap()
            .strategy;
        let strategy_sender = &self.queues[strategy as usize];
        let res = strategy_sender.try_send(msg);
        res.is_ok()
    }
}

impl ProcessorSender for rtrb::Producer<HandledMessage> {
    fn send(&mut self, msg: HandledMessage) -> bool {
        loop {
            if !self.is_full() {
                break;
            }
        }
        self.push(msg).is_ok()
    }
}
