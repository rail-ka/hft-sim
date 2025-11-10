use crate::{
    traits::*,
    types::{HandledMessage, Message},
};

impl StrategyReceiver for rtrb::Consumer<HandledMessage> {
    fn next(&mut self) -> Option<HandledMessage> {
        // if self.is_empty() && self.is_abandoned() {
        //     std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
        //     info!("StrategyReceiver stopped");
        //     return None;
        // }
        let mut count = 0;
        loop {
            count += 1;
            if !self.is_empty() {
                break;
            }
            if count > 1000 && self.is_abandoned() {
                std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
                info!("StrategyReceiver stopped");
                return None;
            }
        }
        let msg = self.pop().unwrap();
        Some(msg)
    }

    fn len(&self) -> usize {
        self.slots()
    }
}

impl ProcessorReceiver for rtrb::Consumer<Message> {
    fn next(&mut self) -> Option<Message> {
        // if self.is_empty() && self.is_abandoned() {
        //     std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
        //     info!("ProcessorReceiver stopped");
        //     return None;
        // }
        let mut count = 0;
        loop {
            count += 1;
            if !self.is_empty() {
                break;
            }
            if count > 1000 && self.is_abandoned() {
                std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
                info!("ProcessorReceiver stopped");
                return None;
            }
        }
        let msg = self.pop().unwrap();
        Some(msg)
    }

    fn len(&self) -> usize {
        self.slots()
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

impl ProducerSender for rtrb::Producer<Message> {
    fn send(&mut self, msg: Message) -> bool {
        loop {
            if !self.is_full() {
                break;
            }
        }
        self.push(msg).is_ok()
    }
}
