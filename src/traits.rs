use crossbeam_channel::Receiver;

use crate::types::HandledMessage;

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
