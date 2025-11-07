use std::{thread::sleep, time::Duration};

use crossbeam_channel::Receiver;
use quanta::{Clock, IntoNanoseconds};

use crate::types::{HandledMesage, Message};

pub type SrategyReceiver = Receiver<HandledMesage>;

pub struct StrategyWorker {
    pub receiver: SrategyReceiver,
    pub processing_time: u64,
}

impl StrategyWorker {
    pub fn run(self) {
        let Self {
            receiver,
            processing_time,
        } = self;
        let mut seq_arr = [[0u64; 8]; 8];
        let clock = Clock::new();

        while let Ok(item) = receiver.recv() {
            let HandledMesage {
                msg:
                    Message {
                        ty,
                        producer_id,
                        seq,
                        timestamp,
                    },
                processor_id,
                processing_ts,
            } = item;
            let instant = clock.now();

            let last = &mut seq_arr[processor_id as usize][ty as usize];
            if *last > seq {
                error!("{last} > {seq}");
            }
            *last = seq;
            let nanos = processing_time.saturating_sub(instant.elapsed().into_nanos());
            sleep(Duration::from_nanos(nanos));
        }
    }
}
