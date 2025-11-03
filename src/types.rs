#[derive(Debug, Clone, Copy)]
pub struct Message {
    /// 0-7
    pub ty: u64,
    pub producer_id: u64,
    /// incrementing per producer
    pub seq: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct HandledMesage {
    pub msg: Message,
    pub processor_id: u64,
    pub processing_ts: u64,
}
