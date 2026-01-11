use env_logger::Env;
use std::io::Write;

#[macro_export]
macro_rules! current_function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        type_name_of(f)
            .rsplit("::")
            .find(|&part| part != "f" && part != "{{closure}}")
            .expect("Failed to find function name")
    }};
}

pub fn init_logging() {
    match env_logger::Builder::from_env(Env::default())
        .format(|buf, record| {
            use crate::request_context::get_request_id;

            // Split string on a character and take the last part
            fn take_last(s: &str, c: char) -> &str {
                s.split(c).next_back().unwrap_or("")
            }

            // Format the target to remove "liboxen::" prefix and replace "::" with "/"
            fn format_target(target: &str) -> String {
                target
                    .strip_prefix("liboxen::")
                    .unwrap_or(target)
                    .rsplit_once("::")
                    .map(|(path, _)| path.replace("::", "/"))
                    .unwrap_or_else(|| target.replace("::", "/"))
            }

            let formatted_target = format_target(record.target());
            let file_name = take_last(record.file().unwrap_or("unknown"), '/');
            let line_number = record.line().unwrap_or(0);

            // Add request ID if available
            let request_id_str = if let Some(request_id) = get_request_id() {
                format!(" [request_id={}]", request_id)
            } else {
                String::new()
            };

            writeln!(
                buf,
                "[{}] {} - {}/{}:{}{}  {}",
                record.level(),
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                formatted_target,
                file_name,
                line_number,
                request_id_str,
                record.args()
            )
        })
        .try_init()
    {
        Ok(_) => (),
        Err(_) => {
            // We already initialized the logger in tests
        }
    }
}
