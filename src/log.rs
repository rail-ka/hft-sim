use tracing_subscriber::fmt::{self, time::Uptime};

pub fn init() {
    let format = fmt::format()
        .compact()
        .with_ansi(true)
        .with_line_number(true)
        .with_target(true)
        .with_level(true)
        .with_timer(Uptime::default());

    tracing_subscriber::fmt().event_format(format).init();
}
