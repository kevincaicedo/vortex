const ASCII_ZERO_BROADCAST: u64 = 0x3030_3030_3030_3030;
const DIGIT_OVERFLOW_BIAS: u64 = 0x7676_7676_7676_7676;
const BYTE_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const EVEN_BYTE_MASK: u64 = 0x00FF_00FF_00FF_00FF;
const EVEN_HALFWORD_MASK: u64 = 0x0000_FFFF_0000_FFFF;
const LOW_U32_MASK: u64 = 0x0000_0000_FFFF_FFFF;
const NON_DIGIT_SENTINEL: u8 = b':';
const I64_MIN_MAGNITUDE: u64 = (i64::MAX as u64) + 1;
const MAX_I64_DIGITS: usize = 19;

/// Parses an ASCII integer prefix using SWAR for the first eight digits.
///
/// Returns the parsed value and the number of bytes consumed. Parsing stops at
/// the first non-digit after an optional sign. Overflow returns `None`.
#[must_use]
#[inline]
pub fn swar_parse_int(buf: &[u8]) -> Option<(i64, usize)> {
    let first = *buf.first()?;
    let negative = first == b'-';
    let explicit_plus = first == b'+';
    let sign_len = usize::from(negative || explicit_plus);

    let (magnitude, digit_len) = parse_unsigned_prefix(&buf[sign_len..])?;
    let consumed = sign_len + digit_len;

    if negative {
        if magnitude == I64_MIN_MAGNITUDE {
            Some((i64::MIN, consumed))
        } else {
            i64::try_from(magnitude).ok().map(|value| (-value, consumed))
        }
    } else {
        i64::try_from(magnitude).ok().map(|value| (value, consumed))
    }
}

#[inline(always)]
fn parse_unsigned_prefix(buf: &[u8]) -> Option<(u64, usize)> {
    let word = load_padded_chunk(buf);
    let digit_values = word.wrapping_sub(ASCII_ZERO_BROADCAST);
    let digit_count = leading_digit_count(digit_values);

    if digit_count == 0 {
        return None;
    }

    let mut value = parse_chunk_value(digit_values, digit_count);
    if digit_count < 8 {
        return Some((value, digit_count));
    }

    let mut consumed = 8;
    while consumed < buf.len() {
        let digit = buf[consumed].wrapping_sub(b'0');
        if digit > 9 {
            break;
        }

        if consumed == MAX_I64_DIGITS {
            return None;
        }

        value = value.checked_mul(10)?;
        value = value.checked_add(u64::from(digit))?;
        consumed += 1;
    }

    Some((value, consumed))
}

#[inline(always)]
fn load_padded_chunk(buf: &[u8]) -> u64 {
    if let Some(chunk) = buf.first_chunk::<8>() {
        u64::from_le_bytes(*chunk)
    } else {
        let mut padded = [NON_DIGIT_SENTINEL; 8];
        let len = buf.len().min(8);
        padded[..len].copy_from_slice(&buf[..len]);
        u64::from_le_bytes(padded)
    }
}

#[inline(always)]
fn leading_digit_count(digit_values: u64) -> usize {
    let invalid = (digit_values | digit_values.wrapping_add(DIGIT_OVERFLOW_BIAS)) & BYTE_HIGH_BITS;
    if invalid == 0 {
        8
    } else {
        (invalid.trailing_zeros() as usize) >> 3
    }
}

#[inline(always)]
fn parse_chunk_value(digit_values: u64, digit_count: usize) -> u64 {
    let mask = valid_byte_mask(digit_count);
    let aligned = (digit_values & mask) << ((8 - digit_count) * 8);

    let even = aligned & EVEN_BYTE_MASK;
    let odd = (aligned >> 8) & EVEN_BYTE_MASK;
    let pairs = even * 10 + odd;

    let low_pairs = pairs & EVEN_HALFWORD_MASK;
    let high_pairs = (pairs >> 16) & EVEN_HALFWORD_MASK;
    let quads = low_pairs * 100 + high_pairs;

    let low_quad = quads & LOW_U32_MASK;
    let high_quad = quads >> 32;
    low_quad * 10_000 + high_quad
}

#[inline(always)]
fn valid_byte_mask(digit_count: usize) -> u64 {
    match digit_count {
        0 => 0,
        8 => u64::MAX,
        _ => (1_u64 << (digit_count * 8)) - 1,
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::string::string_regex;

    use super::*;

    fn scalar_parse_int(buf: &[u8]) -> Option<(i64, usize)> {
        let first = *buf.first()?;
        let sign_len = usize::from(first == b'-' || first == b'+');
        let negative = first == b'-';

        let mut end = sign_len;
        while end < buf.len() && buf[end].is_ascii_digit() {
            end += 1;
        }

        if end == sign_len {
            return None;
        }

        let text = std::str::from_utf8(&buf[..end]).ok()?;
        text.parse::<i64>().ok().map(|value| (value, end)).or_else(|| {
            if negative && text == "-9223372036854775808" {
                Some((i64::MIN, end))
            } else {
                None
            }
        })
    }

    #[test]
    fn parses_single_digits() {
        for digit in b'0'..=b'9' {
            let input = [digit];
            let expected = i64::from(digit - b'0');
            assert_eq!(swar_parse_int(&input), Some((expected, 1)));
        }
    }

    #[test]
    fn parses_multi_digit_values() {
        let cases = [
            (b"42".as_slice(), 42_i64),
            (b"1234".as_slice(), 1234_i64),
            (b"65535".as_slice(), 65_535_i64),
            (b"2147483647".as_slice(), 2_147_483_647_i64),
            (b"9223372036854775807".as_slice(), i64::MAX),
        ];

        for (input, expected) in cases {
            assert_eq!(swar_parse_int(input), Some((expected, input.len())));
        }
    }

    #[test]
    fn parses_negative_values() {
        let cases = [
            (b"-1".as_slice(), -1_i64),
            (b"-100".as_slice(), -100_i64),
            (b"-9223372036854775808".as_slice(), i64::MIN),
        ];

        for (input, expected) in cases {
            assert_eq!(swar_parse_int(input), Some((expected, input.len())));
        }
    }

    #[test]
    fn accepts_explicit_plus_sign() {
        assert_eq!(swar_parse_int(b"+42"), Some((42, 3)));
    }

    #[test]
    fn parses_leading_zeroes() {
        assert_eq!(swar_parse_int(b"007"), Some((7, 3)));
    }

    #[test]
    fn stops_at_first_non_digit() {
        assert_eq!(swar_parse_int(b"12a4"), Some((12, 2)));
        assert_eq!(swar_parse_int(b"-98\r\n"), Some((-98, 3)));
    }

    #[test]
    fn rejects_empty_or_sign_only_inputs() {
        assert_eq!(swar_parse_int(b""), None);
        assert_eq!(swar_parse_int(b"-"), None);
        assert_eq!(swar_parse_int(b"+"), None);
        assert_eq!(swar_parse_int(b"x123"), None);
    }

    #[test]
    fn rejects_overflow() {
        assert_eq!(swar_parse_int(b"9223372036854775808"), None);
        assert_eq!(swar_parse_int(b"99999999999999999999"), None);
        assert_eq!(swar_parse_int(b"-9223372036854775809"), None);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1_000))]

        #[test]
        fn valid_integer_strings_match_rust_parse(input in string_regex("[+-]?[0-9]{1,19}").unwrap()) {
            let expected = input.parse::<i64>().ok().map(|value| (value, input.len()));
            prop_assert_eq!(swar_parse_int(input.as_bytes()), expected);
        }

        #[test]
        fn arbitrary_inputs_match_scalar_reference(input in proptest::collection::vec(any::<u8>(), 0..64)) {
            prop_assert_eq!(swar_parse_int(&input), scalar_parse_int(&input));
        }
    }
}