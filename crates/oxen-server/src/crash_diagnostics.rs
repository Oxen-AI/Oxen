//! Log backtraces on panic and fatal signals so CloudWatch captures the site of
//! abrupt exits (SIGBUS, SIGSEGV, SIGABRT) that otherwise leave no application log.

use std::backtrace::Backtrace;
use std::io::{self, Write};
use std::panic;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

#[cfg(unix)]
use signal_hook::consts::signal::{SIGABRT, SIGBUS, SIGSEGV};

static FATAL_SIGNAL_LOGGED: AtomicBool = AtomicBool::new(false);

fn log_backtrace(label: &str) {
    let mut stderr = io::stderr().lock();
    let _ = writeln!(stderr, "{label}");
    let _ = writeln!(stderr, "{:?}", Backtrace::force_capture());
}

fn install_panic_hook() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        log_backtrace(&format!("oxen-server panic: {info}"));
        default_hook(info);
    }));
}

#[cfg(unix)]
fn install_fatal_signal_handler() {
    thread::spawn(|| {
        let Ok(mut signals) = signal_hook::iterator::Signals::new([SIGBUS, SIGSEGV, SIGABRT])
        else {
            return;
        };

        for signal in signals.forever() {
            if FATAL_SIGNAL_LOGGED
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                unsafe {
                    libc::raise(signal);
                }
            }

            let name = match signal {
                SIGBUS => "SIGBUS",
                SIGSEGV => "SIGSEGV",
                SIGABRT => "SIGABRT",
                _ => "SIGNAL",
            };
            log_backtrace(&format!(
                "oxen-server received fatal signal {name} ({signal})"
            ));

            unsafe {
                libc::signal(signal, libc::SIG_DFL);
                libc::raise(signal);
            }
        }
    });
}

#[cfg(not(unix))]
fn install_fatal_signal_handler() {}

pub fn install() {
    install_panic_hook();
    install_fatal_signal_handler();
}
