use crate::state::State;
use std::{thread, time::Duration};
use tracing::{info, warn};

pub fn spawn_worker<F, T>(
    name: String,
    core_ids: &mut Vec<core_affinity::CoreId>,
    worker_fn: F,
) -> thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let cid = core_ids.pop();
    thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            if let Some(cid) = cid
                && !core_affinity::set_for_current(cid)
            {
                warn!("cannot pin {cid:?} thread for {name}");
            }
            worker_fn()
        })
        .unwrap()
}

pub fn main_loop(duration_secs: u64, state: &mut State) {
    for sec in 0..duration_secs {
        thread::sleep(Duration::from_secs(1));
        info!(sec);
        state.print_histogram();
    }
}
