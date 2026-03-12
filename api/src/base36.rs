const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

pub fn encode(mut num: i64) -> String {
    if num == 0 {
        return "0".to_string();
    }

    let mut result = Vec::new();
    while num > 0 {
        result.push(ALPHABET[(num % 36) as usize]);
        num /= 36;
    }
    result.reverse();
    String::from_utf8(result).unwrap()
}

pub fn decode(s: &str) -> Option<i64> {
    let s = s.to_lowercase();
    let mut result: i64 = 0;
    for c in s.chars() {
        let digit = match c {
            '0'..='9' => c as i64 - '0' as i64,
            'a'..='z' => c as i64 - 'a' as i64 + 10,
            _ => return None,
        };
        result = result.checked_mul(36)?.checked_add(digit)?;
    }
    Some(result)
}
