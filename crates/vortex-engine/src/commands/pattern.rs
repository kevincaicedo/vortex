//! Shared glob pattern matching helpers for key commands.

/// Match a Redis-style glob pattern against a byte string.
///
/// Supported tokens:
/// - `*` any sequence
/// - `?` any single byte
/// - `[abc]` character class
/// - `[^abc]` / `[!abc]` negated character class
/// - `\x` escaped literal
#[inline]
pub(crate) fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut pattern_index = 0usize;
    let mut string_index = 0usize;
    let mut star_pattern_index = usize::MAX;
    let mut star_string_index = usize::MAX;

    while string_index < string.len() {
        if pattern_index < pattern.len() {
            match pattern[pattern_index] {
                b'*' => {
                    star_pattern_index = pattern_index + 1;
                    star_string_index = string_index;
                    pattern_index += 1;
                    continue;
                }
                b'?' => {
                    pattern_index += 1;
                    string_index += 1;
                    continue;
                }
                b'[' => {
                    pattern_index += 1;
                    let negate = pattern_index < pattern.len()
                        && (pattern[pattern_index] == b'^' || pattern[pattern_index] == b'!');
                    if negate {
                        pattern_index += 1;
                    }

                    let mut found = false;
                    let mut class_end = pattern_index;
                    while class_end < pattern.len() && pattern[class_end] != b']' {
                        if class_end + 2 < pattern.len()
                            && pattern[class_end + 1] == b'-'
                            && pattern[class_end + 2] != b']'
                        {
                            if string[string_index] >= pattern[class_end]
                                && string[string_index] <= pattern[class_end + 2]
                            {
                                found = true;
                            }
                            class_end += 3;
                        } else {
                            if string[string_index] == pattern[class_end] {
                                found = true;
                            }
                            class_end += 1;
                        }
                    }

                    if class_end < pattern.len() {
                        class_end += 1;
                    }
                    if negate {
                        found = !found;
                    }
                    if found {
                        pattern_index = class_end;
                        string_index += 1;
                        continue;
                    }
                }
                b'\\' => {
                    pattern_index += 1;
                    if pattern_index < pattern.len()
                        && pattern[pattern_index] == string[string_index]
                    {
                        pattern_index += 1;
                        string_index += 1;
                        continue;
                    }
                }
                byte => {
                    if byte == string[string_index] {
                        pattern_index += 1;
                        string_index += 1;
                        continue;
                    }
                }
            }
        }

        if star_pattern_index != usize::MAX {
            pattern_index = star_pattern_index;
            star_string_index += 1;
            string_index = star_string_index;
            continue;
        }

        return false;
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    #[test]
    fn matches_wildcards() {
        assert!(glob_match(b"*", b"hello"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"user:*", b"user:42"));
        assert!(!glob_match(b"user:*", b"post:42"));
    }

    #[test]
    fn matches_classes() {
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hxllo"));
        assert!(glob_match(b"h[^ae]llo", b"hxllo"));
        assert!(!glob_match(b"h[^ae]llo", b"hello"));
    }
}
