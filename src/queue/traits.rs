use crossbeam_channel::{Receiver, Sender};

use crate::{
    Arr,
    config::Stage2Rule,
    traits::*,
    types::{HandledMessage, Message},
};

impl StrategyReceiver for Receiver<HandledMessage> {
    fn next(&mut self) -> Option<HandledMessage> {
        self.recv().ok()
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl ProcessorReceiver for Receiver<Message> {
    fn next(&mut self) -> Option<Message> {
        self.recv().ok()
    }
    fn len(&self) -> usize {
        self.len()
    }
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

impl ProducerSender for super::stage1::Stage1Queue {
    fn send(&mut self, msg: Message) -> bool {
        super::stage1::Stage1Queue::send(self, msg)
    }
}
