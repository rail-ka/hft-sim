use crossbeam_channel::Sender;
use itertools::Itertools;

use crate::{config::Stage1Rule, types::Message};

#[derive(Clone)]
pub struct Stage1 {
    msg_routes: Vec<Vec<Sender<Message>>>,
}

impl Stage1 {
    pub fn new(processors: Vec<Sender<Message>>, stage1_rules: Vec<Stage1Rule>) -> Self {
        let msg_routes = stage1_rules
            .into_iter()
            .sorted_by_key(|i| i.msg_type)
            .map(|i| {
                i.processors
                    .into_iter()
                    .map(|i| processors[i as usize].clone())
                    .collect_vec()
            })
            .collect_vec();
        Self { msg_routes }
    }

    pub fn send(&self, msg: Message) -> bool {
        let rule = &self.msg_routes[msg.ty as usize];
        if rule.len() == 1 {
            return rule.first().unwrap().try_send(msg).is_ok();
        }
        let index = (msg.producer_id as usize) % rule.len();
        rule[index].try_send(msg).is_ok()
        // rule.iter()
        //     .min_by_key(|q| q.len())
        //     .unwrap()
        //     .try_send(msg)
        //     .is_ok()
    }
}
