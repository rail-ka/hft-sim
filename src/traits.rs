use crate::types::{HandledMessage, Message};

pub trait StrategyReceiver {
    fn next(&mut self) -> Option<HandledMessage>;
    fn len(&self) -> usize;
}

pub trait ProcessorReceiver {
    fn next(&mut self) -> Option<Message>;
    fn len(&self) -> usize;
}

pub trait ProcessorSender {
    fn send(&mut self, msg: HandledMessage) -> bool;
}

pub trait ProducerSender {
    fn send(&mut self, msg: Message) -> bool;
}
