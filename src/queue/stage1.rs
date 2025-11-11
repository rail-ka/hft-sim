use crossbeam_channel::Sender;
use itertools::Itertools;

use crate::{Arr, config::Stage1Rule, types::Message};

#[derive(Clone)]
pub struct Stage1Queue {
    msg_routes: Arr<Arr<Sender<Message>>>,
}

impl Stage1Queue {
    pub fn new(processors: Vec<Sender<Message>>, stage1_rules: Vec<Stage1Rule>) -> Self {
        let msg_routes = stage1_rules
            .into_iter()
            .sorted_by_key(|i| i.msg_type)
            .map(|i| {
                i.processors
                    .into_iter()
                    .map(|i| processors[i as usize].clone())
                    .collect()
            })
            .collect();
        Self { msg_routes }
    }

    pub fn send(&self, msg: Message) -> bool {
        let rule = &self.msg_routes[msg.ty as usize];
        if rule.len() == 1 {
            return rule.first().unwrap().try_send(msg).is_ok();
        }
        // не документировано, что может быть несколько, но в конфиге массив processors
        let index = (msg.producer_id as usize) % rule.len();
        rule[index].try_send(msg).is_ok()
        // TODO: if we need balancing (but not ordering reguired!):
        // rule.iter()
        //     .min_by_key(|q| q.len())
        //     .unwrap()
        //     .try_send(msg)
        //     .is_ok()
    }
}
