pub fn split_and_trim(input: &str, delimiter: &str) -> Vec<String> {
    input
        .split(delimiter)
        .map(|v| v.trim())
        .map(String::from)
        .collect::<Vec<String>>()
}

/// Converts a number to its ordinal string representation (1st, 2nd, 3rd, etc.)
pub fn to_ordinal(n: u64) -> String {
    let suffix = match (n % 10, n % 100) {
        (1, 11) | (2, 12) | (3, 13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("{n}{suffix}")
}
