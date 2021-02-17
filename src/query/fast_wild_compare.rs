// Ported from http://developforperformance.com/MatchingWildcards_AnImprovedAlgorithmForBigData.html

const ASTERISK: u8 = 42;
const QUESTION_MARK: u8 = 63;

pub fn fast_wild_compare_portable(str_tame: &str, str_wild: &str) -> bool {
    let str_wild = str_wild.as_bytes();
    let str_tame = str_tame.as_bytes();
    let mut i_wild = 0; // Index for both tame and wild strings in upper loop
    let mut i_tame; // Index for tame string, set going into lower loop
    let mut i_wild_sequence; // Index for prospective match after '*' (wild string)
    let mut i_tame_sequence; // Index for prospective match (tame string)

    // Find a first wildcard, if one exists, and the beginning of any
    // prospectively matching sequence after it.
    loop {
        // Check for the end from the start.  Get out fast, if possible.
        if str_tame.get(i_wild).is_none() {
            return if str_wild.get(i_wild).is_some() {
                while str_wild.get(i_wild) == Some(&ASTERISK) {
                    i_wild += 1;
                    if str_wild.get(i_wild).is_none() {
                        return true; // "ab" matches "ab*".
                    }
                }
                false // "abcd" doesn't match "abc".
            } else {
                true // "abc" matches "abc".
            };
        } else if str_wild.get(i_wild) == Some(&ASTERISK) {
            // Got wild: set up for the second loop and skip on down there.
            i_tame = i_wild;

            loop {
                i_wild += 1;
                if str_wild.get(i_wild) != Some(&ASTERISK) {
                    break;
                }
            }

            if str_wild.get(i_wild).is_none() {
                return true; // "abc*" matches "abcd".
            }

            // Search for the next prospective match.
            if str_wild.get(i_wild) != Some(&QUESTION_MARK) {
                while str_wild.get(i_wild) != str_tame.get(i_tame) {
                    i_tame += 1;
                    if str_tame.get(i_tame).is_none() {
                        return false; // "a*bc" doesn't match "ab".
                    }
                }
            }

            // Keep fallback positions for retry in case of incomplete match.
            i_wild_sequence = i_wild;
            i_tame_sequence = i_tame;
            break;
        } else if str_wild.get(i_wild) != str_tame.get(i_wild)
            && str_wild.get(i_wild) != Some(&QUESTION_MARK)
        {
            return false; // "abc" doesn't match "abd".
        }
        i_wild += 1;
    } // Everything's a match, so far.

    loop {
        if str_wild.get(i_wild) == Some(&ASTERISK) {
            // Got wild again.
            i_wild += 1;
            while str_wild.get(i_wild) == Some(&ASTERISK) {
                i_wild += 1;
            }

            if str_wild.get(i_wild).is_none() {
                return true; // "ab*c*" matches "abcd".
            }

            if str_tame.get(i_tame).is_none() {
                return false; // "*bcd*" doesn't match "abc".
            }

            // Search for the next prospective match.
            if str_wild.get(i_wild) != Some(&QUESTION_MARK) {
                while str_wild.get(i_wild) != str_tame.get(i_tame) {
                    i_tame += 1;
                    if str_tame.get(i_tame).is_none() {
                        return false; // "a*b*c" doesn't match "ab".
                    }
                }
            }

            // Keep the new fallback positions.
            i_wild_sequence = i_wild;
            i_tame_sequence = i_tame;
        } else if str_wild.get(i_wild) != str_tame.get(i_tame)
            && str_wild.get(i_wild) != Some(&QUESTION_MARK)
        {
            // The equivalent portion of the upper loop is really simple.
            if str_tame.get(i_tame).is_none() {
                return false; // "*bcd" doesn't match "abc".
            }

            // A fine time for questions.
            while str_wild.get(i_wild_sequence) == Some(&QUESTION_MARK) {
                i_wild_sequence += 1;
                i_tame_sequence += 1;
            }

            i_wild = i_wild_sequence;

            // Fall back, but never so far again.
            i_tame_sequence += 1;
            while str_wild.get(i_wild) != str_tame.get(i_tame_sequence) {
                if str_tame.get(i_tame_sequence).is_none() {
                    return false; // "*a*b" doesn't match "ac".
                }
                i_tame_sequence += 1;
            }

            i_tame = i_tame_sequence;
        }

        // Another check for the end, at the end.
        if str_tame.get(i_tame).is_none() {
            return str_wild.get(i_wild).is_none(); // "*bc" matches "abc"
        }

        i_wild += 1; // Everything's still a match.
        i_tame += 1;
    }
}

#[cfg(test)]
mod tests {
    use crate::query::fast_wild_compare::fast_wild_compare_portable;

    #[test]
    fn test_wild() {
        let wild_cases = vec![
            // Case with first wildcard after total match.
            ("Hi", "Hi*", true),

            // Case with mismatch after '*'
            ("abc", "ab*d", false),

            // Cases with repeating character sequences.
            ("abcccd", "*ccd", true),
            ("mississipissippi", "*issip*ss*", true),
            ("xxxx*zzzzzzzzy*f", "xxxx*zzy*fffff", false),
            ("xxxx*zzzzzzzzy*f", "xxx*zzy*f", true),
            ("xxxxzzzzzzzzyf", "xxxx*zzy*fffff", false),
            ("xxxxzzzzzzzzyf", "xxxx*zzy*f", true),
            ("xyxyxyzyxyz", "xy*z*xyz", true),
            ("mississippi", "*sip*", true),
            ("xyxyxyxyz", "xy*xyz", true),
            ("mississippi", "mi*sip*", true),
            ("ababac", "*abac*", true),
            ("ababac", "*abac*", true),
            ("aaazz", "a*zz*", true),
            ("a12b12", "*12*23", false),
            ("a12b12", "a12b", false),
            ("a12b12", "*12*12*", true),

            // From DDJ reader Andy Belf
            ("caaab", "*a?b", true),

            // Additional cases where the '*' char appears in the tame string.
            ("*", "*", true),
            ("a*abab", "a*b", true),
            ("a*r", "a*", true),
            ("a*ar", "a*aar", false),

            // More double wildcard scenarios.
            ("XYXYXYZYXYz", "XY*Z*XYz", true),
            ("missisSIPpi", "*SIP*", true),
            ("mississipPI", "*issip*PI", true),
            ("xyxyxyxyz", "xy*xyz", true),
            ("miSsissippi", "mi*sip*", true),
            ("miSsissippi", "mi*Sip*", false),
            ("abAbac", "*Abac*", true),
            ("abAbac", "*Abac*", true),
            ("aAazz", "a*zz*", true),
            ("A12b12", "*12*23", false),
            ("a12B12", "*12*12*", true),
            ("oWn", "*oWn*", true),

            // Completely tame (no wildcards) cases.
            ("bLah", "bLah", true),
            ("bLah", "bLaH", false),

            // Simple mixed wildcard tests suggested by Marlin Deckert.
            ("a", "*?", true),
            ("ab", "*?", true),
            ("abc", "*?", true),

            // More mixed wildcard tests including coverage for false positives.
            ("a", "??", false),
            ("ab", "?*?", true),
            ("ab", "*?*?*", true),
            ("abc", "?**?*?", true),
            ("abc", "?**?*&?", false),
            ("abcd", "?b*??", true),
            ("abcd", "?a*??", false),
            ("abcd", "?**?c?", true),
            ("abcd", "?**?d?", false),
            ("abcde", "?*b*?*d*?", true),

            // Single-character-match cases.
            ("bLah", "bL?h", true),
            ("bLaaa", "bLa?", false),
            ("bLah", "bLa?", true),
            ("bLaH", "?Lah", false),
            ("bLaH", "?LaH", true),

            // Many-wildcard scenarios.
            ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", "a*a*a*a*a*a*aa*aaa*a*a*b", true),
            ("abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "*a*b*ba*ca*a*aa*aaa*fa*ga*b*", true),
            ("abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "*a*b*ba*ca*a*x*aaa*fa*ga*b*", false),
            ("abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "*a*b*ba*ca*aaaa*fa*ga*gggg*b*", false),
            ("abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab", "*a*b*ba*ca*aaaa*fa*ga*ggg*b*", true),
            ("aaabbaabbaab", "*aabbaa*a*", true),
            ("a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*", "a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*", true),
            ("aaaaaaaaaaaaaaaaa", "*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*", true),
            ("aaaaaaaaaaaaaaaa", "*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*", false),
            ("abc*abcd*abcde*abcdef*abcdefg*abcdefgh*abcdefghi*abcdefghij*abcdefghijk*abcdefghijkl*abcdefghijklm*abcdefghijklmn", "abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*", false),
            ("abc*abcd*abcde*abcdef*abcdefg*abcdefgh*abcdefghi*abcdefghij*abcdefghijk*abcdefghijkl*abcdefghijklm*abcdefghijklmn", "abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*", true),
            ("abc*abcd*abcd*abc*abcd", "abc*abc*abc*abc*abc", false),
            ("abc*abcd*abcd*abc*abcd*abcd*abc*abcd*abc*abc*abcd", "abc*abc*abc*abc*abc*abc*abc*abc*abc*abc*abcd", true),
            ("abc", "********a********b********c********", true),
            ("********a********b********c********", "abc", false),
            ("abc", "********a********b********b********", false),
            ("*abc*", "***a*b*c***", true),

            // A case-insensitive algorithm test.
            // ("mississippi", "*issip*PI", true),

            // Tests suggested by other DDJ readers
            ("", "?", false),
            ("", "*?", false),
            ("", "", true),
            ("", "*", true),
            ("a", "", false),
        ];

        for (tame, wild, result) in wild_cases {
            assert_eq!(fast_wild_compare_portable(tame, wild), result);
        }
    }

    #[test]
    fn test_tame() {
        let tame_cases = vec![
            // Case with last character mismatch.
            ("abc", "abd", false),
            // Cases with repeating character sequences.
            ("abcccd", "abcccd", true),
            ("mississipissippi", "mississipissippi", true),
            ("xxxxzzzzzzzzyf", "xxxxzzzzzzzzyfffff", false),
            ("xxxxzzzzzzzzyf", "xxxxzzzzzzzzyf", true),
            ("xxxxzzzzzzzzyf", "xxxxzzy.fffff", false),
            ("xxxxzzzzzzzzyf", "xxxxzzzzzzzzyf", true),
            ("xyxyxyzyxyz", "xyxyxyzyxyz", true),
            ("mississippi", "mississippi", true),
            ("xyxyxyxyz", "xyxyxyxyz", true),
            ("m ississippi", "m ississippi", true),
            ("ababac", "ababac?", false),
            ("dababac", "ababac", false),
            ("aaazz", "aaazz", true),
            ("a12b12", "1212", false),
            ("a12b12", "a12b", false),
            ("a12b12", "a12b12", true),
            // A mix of cases
            ("n", "n", true),
            ("aabab", "aabab", true),
            ("ar", "ar", true),
            ("aar", "aaar", false),
            ("XYXYXYZYXYz", "XYXYXYZYXYz", true),
            ("missisSIPpi", "missisSIPpi", true),
            ("mississipPI", "mississipPI", true),
            ("xyxyxyxyz", "xyxyxyxyz", true),
            ("miSsissippi", "miSsissippi", true),
            ("miSsissippi", "miSsisSippi", false),
            ("abAbac", "abAbac", true),
            ("abAbac", "abAbac", true),
            ("aAazz", "aAazz", true),
            ("A12b12", "A12b123", false),
            ("a12B12", "a12B12", true),
            ("oWn", "oWn", true),
            ("bLah", "bLah", true),
            ("bLah", "bLaH", false),
            // Single '?' cases.
            ("a", "a", true),
            ("ab", "a?", true),
            ("abc", "ab?", true),
            // Mixed '?' cases.
            ("a", "??", false),
            ("ab", "??", true),
            ("abc", "???", true),
            ("abcd", "????", true),
            ("abc", "????", false),
            ("abcd", "?b??", true),
            ("abcd", "?a??", false),
            ("abcd", "??c?", true),
            ("abcd", "??d?", false),
            ("abcde", "?b?d*?", true),
            // Longer string scenarios.
            (
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
                true,
            ),
            (
                "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
                "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
                true,
            ),
            (
                "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
                "abababababababababababababababababababaacacacacacacacadaeafagahaiajaxalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
                false,
            ),
            (
                "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
                "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaggggagaaaaaaaab",
                false,
            ),
            (
                "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
                "abababababababababababababababababababaacacacacacacacadaeafagahaiajakalaaaaaaaaaaaaaaaaaffafagaagggagaaaaaaaab",
                true,
            ),
            ("aaabbaabbaab", "aaabbaabbaab", true),
            ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true,
            ),
            ("aaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaa", true),
            ("aaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaa", false),
            (
                "abcabcdabcdeabcdefabcdefgabcdefghabcdefghiabcdefghijabcdefghijkabcdefghijklabcdefghijklmabcdefghijklmn",
                "abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc",
                false,
            ),
            (
                "abcabcdabcdeabcdefabcdefgabcdefghabcdefghiabcdefghijabcdefghijkabcdefghijklabcdefghijklmabcdefghijklmn",
                "abcabcdabcdeabcdefabcdefgabcdefghabcdefghiabcdefghijabcdefghijkabcdefghijklabcdefghijklmabcdefghijklmn",
                true,
            ),
            ("abcabcdabcdabcabcd", "abcabc?abcabcabc", false),
            (
                "abcabcdabcdabcabcdabcdabcabcdabcabcabcd",
                "abcabc?abc?abcabc?abc?abc?bc?abc?bc?bcd",
                true,
            ),
            ("?abc?", "?abc?", true),
        ];

        for (tame, wild, result) in tame_cases {
            assert_eq!(fast_wild_compare_portable(tame, wild), result);
        }
    }

    #[test]
    fn test_empty() {
        let empty_cases = vec![
            // A simple case
            ("", "abd", false),
            // Cases with repeating character sequences
            ("", "abcccd", false),
            ("", "mississipissippi", false),
            ("", "xxxxzzzzzzzzyfffff", false),
            ("", "xxxxzzzzzzzzyf", false),
            ("", "xxxxzzy.fffff", false),
            ("", "xxxxzzzzzzzzyf", false),
            ("", "xyxyxyzyxyz", false),
            ("", "mississippi", false),
            ("", "xyxyxyxyz", false),
            ("", "m ississippi", false),
            ("", "ababac*", false),
            ("", "ababac", false),
            ("", "aaazz", false),
            ("", "1212", false),
            ("", "a12b", false),
            ("", "a12b12", false),
            // A mix of cases
            ("", "n", false),
            ("", "aabab", false),
            ("", "ar", false),
            ("", "aaar", false),
            ("", "XYXYXYZYXYz", false),
            ("", "missisSIPpi", false),
            ("", "mississipPI", false),
            ("", "xyxyxyxyz", false),
            ("", "miSsissippi", false),
            ("", "miSsisSippi", false),
            ("", "abAbac", false),
            ("", "abAbac", false),
            ("", "aAazz", false),
            ("", "A12b123", false),
            ("", "a12B12", false),
            ("", "oWn", false),
            ("", "bLah", false),
            ("", "bLaH", false),
            // Both strings empty
            ("", "", true),
            // Another simple case
            ("abc", "", false),
            // Cases with repeating character sequences.
            ("abcccd", "", false),
            ("mississipissippi", "", false),
            ("xxxxzzzzzzzzyf", "", false),
            ("xxxxzzzzzzzzyf", "", false),
            ("xxxxzzzzzzzzyf", "", false),
            ("xxxxzzzzzzzzyf", "", false),
            ("xyxyxyzyxyz", "", false),
            ("mississippi", "", false),
            ("xyxyxyxyz", "", false),
            ("m ississippi", "", false),
            ("ababac", "", false),
            ("dababac", "", false),
            ("aaazz", "", false),
            ("a12b12", "", false),
            ("a12b12", "", false),
            ("a12b12", "", false),
            // A mix of cases
            ("n", "", false),
            ("aabab", "", false),
            ("ar", "", false),
            ("aar", "", false),
            ("XYXYXYZYXYz", "", false),
            ("missisSIPpi", "", false),
            ("mississipPI", "", false),
            ("xyxyxyxyz", "", false),
            ("miSsissippi", "", false),
            ("miSsissippi", "", false),
            ("abAbac", "", false),
            ("abAbac", "", false),
            ("aAazz", "", false),
            ("A12b12", "", false),
            ("a12B12", "", false),
            ("oWn", "", false),
            ("bLah", "", false),
            ("bLah", "", false),
        ];

        for (tame, wild, result) in empty_cases {
            assert_eq!(fast_wild_compare_portable(tame, wild), result);
        }
    }
}
