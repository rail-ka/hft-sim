use crossbeam_channel::{Receiver, Sender};

use crate::types::{HandledMessage, Message};

pub trait ProducerSender {
    fn send(&self, msg: Message);
}

pub trait StrategyReceiver {
    fn recv(&self) -> Option<HandledMessage>;
    fn len(&self) -> usize;
}

impl StrategyReceiver for Receiver<HandledMessage> {
    fn recv(&self) -> Option<HandledMessage> {
        self.recv().ok()
    }
    fn len(&self) -> usize {
        self.len()
    }
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
