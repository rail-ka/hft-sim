use crossbeam_channel::Sender;

use crate::types::{HandledMessage, Message};

pub trait ProducerSender {
    fn send(&self, msg: Message);
}

pub trait StrategyReceiver {
    fn recv(&self) -> Option<HandledMessage>;
}

pub trait ProcessorChannels {
    fn recv(&self) -> Option<Message>;
    fn send(&self, msg: HandledMessage);
}

impl ProducerSender for Sender<Message> {
    fn send(&self, msg: Message) {
        let res = self.try_send(msg);
    }
}
