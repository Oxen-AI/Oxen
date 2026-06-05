#[derive(Clone, Debug)]
pub struct CountLinesOpts {
    pub with_chars: bool,
    pub remove_trailing_blank_line: bool,
}

impl CountLinesOpts {
    pub fn empty() -> CountLinesOpts {
        CountLinesOpts {
            with_chars: false,
            remove_trailing_blank_line: false,
        }
    }
}
