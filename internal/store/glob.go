package store

// GlobMatch reports whether s matches the glob pattern, using the same syntax
// as Redis' KEYS command. A '*' matches any sequence of characters (including
// none); the other special forms are:
//
//	?      exactly one character
//	[abc]  one of a, b or c        [a-z]  a range        [^abc]  none of these
//	\x     the literal character x
//
// It is iterative (it remembers only the last '*'), so patterns such as
// "*a*a*a*a*b" run in O(len(pattern)*len(s)) instead of exponential time.
func GlobMatch(pattern, s string) bool {
	px, sx := 0, 0
	starPx, starSx := -1, 0
	for sx < len(s) {
		if px < len(pattern) {
			switch c := pattern[px]; c {
			case '*':
				for px < len(pattern) && pattern[px] == '*' {
					px++
				}
				if px == len(pattern) {
					return true
				}
				starPx, starSx = px, sx
				continue
			case '?':
				px++
				sx++
				continue
			case '[':
				if ok, next := matchClass(pattern, px, s[sx]); ok {
					px = next
					sx++
					continue
				}
			case '\\':
				if px+1 < len(pattern) {
					if pattern[px+1] == s[sx] {
						px += 2
						sx++
						continue
					}
				} else if c == s[sx] { // a trailing backslash is a literal backslash
					px++
					sx++
					continue
				}
			default:
				if c == s[sx] {
					px++
					sx++
					continue
				}
			}
		}
		// Mismatch: retry from the last '*', letting it swallow one more character.
		if starPx >= 0 {
			starSx++
			sx = starSx
			px = starPx
			continue
		}
		return false
	}
	for px < len(pattern) && pattern[px] == '*' {
		px++
	}
	return px == len(pattern)
}

// matchClass matches ch against the bracket expression that starts at
// pattern[i] == '['. It returns whether ch matched and the index right after
// the closing ']'.
func matchClass(pattern string, i int, ch byte) (bool, int) {
	i++ // skip '['
	negate := false
	if i < len(pattern) && pattern[i] == '^' {
		negate = true
		i++
	}
	matched := false
	for i < len(pattern) {
		c := pattern[i]
		switch {
		case c == '\\' && i+1 < len(pattern):
			i++
			if pattern[i] == ch {
				matched = true
			}
			i++
		case c == ']':
			return matched != negate, i + 1
		case i+2 < len(pattern) && pattern[i+1] == '-' && pattern[i+2] != ']':
			lo, hi := c, pattern[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if ch >= lo && ch <= hi {
				matched = true
			}
			i += 3
		default:
			if c == ch {
				matched = true
			}
			i++
		}
	}
	// Unterminated class: it extends to the end of the pattern, like Redis.
	return matched != negate, i
}
