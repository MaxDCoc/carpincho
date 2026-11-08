// Package resp implements RESP2, the Redis Serialization Protocol: a streaming
// request parser (multibulk and inline commands, pipelining) and a buffered
// reply writer.
//
// The parser is written to be safe against hostile input: every length is
// validated against a limit before anything is allocated, large bulk strings
// are read in chunks so a lying length prefix cannot force a huge allocation,
// and nesting depth is bounded. The Go native fuzz tests in fuzz_test.go
// exercise these properties.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
)

// Limits. They match the defaults of Redis 7 (proto-max-bulk-len, the
// multibulk limit of 1024*1024 arguments and the 64KB inline limit).
const (
	MaxBulkLen      = 512 << 20
	MaxMultibulkLen = 1 << 20
	MaxInlineLen    = 64 << 10

	maxDepth     = 32       // nesting limit for replies parsed by ReadValue
	maxHeaderLen = 64       // longest accepted "*<n>", "$<n>" or ":<n>" line
	readChunk    = 64 << 10 // bulk strings above this size are read incrementally
	bufSize      = 16 << 10
)

// ProtocolError reports malformed input. A server answers it with
// "-ERR Protocol error: <Msg>" and closes the connection, exactly like Redis.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.Msg }

func protoErr(msg string) error { return &ProtocolError{Msg: msg} }

var errLineTooLong = errors.New("resp: line too long")

// Reader parses RESP2 from a stream. It is not safe for concurrent use.
type Reader struct{ br *bufio.Reader }

// NewReader returns a Reader that buffers r.
func NewReader(r io.Reader) *Reader { return &Reader{br: bufio.NewReaderSize(r, bufSize)} }

// Buffered returns how many bytes are already read from the underlying stream
// but not yet consumed by the parser.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// eofToUnexpected turns a clean EOF into io.ErrUnexpectedEOF. It is applied to
// every read that happens after the first byte of a message was consumed, so
// callers can tell "the stream ended between commands" (io.EOF) from "the
// stream ended in the middle of a command" (io.ErrUnexpectedEOF). The AOF
// loader relies on that to tolerate a truncated tail.
func eofToUnexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// ReadCommand reads the next client request and returns its arguments.
//
// Two request formats are accepted, like Redis does:
//
//   - multibulk: *<n>\r\n followed by n bulk strings ($<len>\r\n<bytes>\r\n)
//   - inline: a single line split into arguments, with quoting rules
//
// Empty lines and empty multibulks (*0, *-1) are skipped. The returned slices
// are freshly allocated and owned by the caller.
//
// Errors: io.EOF when the stream ends cleanly between commands,
// io.ErrUnexpectedEOF when it ends inside a command, *ProtocolError for
// malformed input, or whatever the underlying reader returned.
func (r *Reader) ReadCommand() ([][]byte, error) {
	for {
		first, err := r.br.Peek(1)
		if err != nil {
			return nil, err
		}
		if first[0] == '*' {
			_, _ = r.br.Discard(1)
			args, err := r.readMultibulk()
			if err != nil {
				return nil, eofToUnexpected(err)
			}
			if args == nil {
				continue // *0 or *-1: nothing to execute
			}
			return args, nil
		}
		line, _, err := r.readLine(MaxInlineLen)
		if err != nil {
			if errors.Is(err, errLineTooLong) {
				return nil, protoErr("too big inline request")
			}
			return nil, eofToUnexpected(err)
		}
		args, err := SplitInline(line)
		if err != nil {
			return nil, err
		}
		if len(args) == 0 {
			continue // blank line
		}
		return args, nil
	}
}

// readLine returns the next line without its terminator and whether the
// terminator was CRLF (true) or a bare LF (false). The slice is only valid
// until the next read. Lines longer than limit yield errLineTooLong.
func (r *Reader) readLine(limit int) ([]byte, bool, error) {
	line, err := r.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		acc := slices.Clone(line)
		for err == bufio.ErrBufferFull {
			if len(acc) > limit {
				return nil, false, errLineTooLong
			}
			line, err = r.br.ReadSlice('\n')
			acc = append(acc, line...)
		}
		if len(acc) > limit {
			return nil, false, errLineTooLong
		}
		line = acc
	}
	if err != nil {
		return nil, false, eofToUnexpected(err)
	}
	line = line[:len(line)-1] // drop \n
	crlf := len(line) > 0 && line[len(line)-1] == '\r'
	if crlf {
		line = line[:len(line)-1]
	}
	return line, crlf, nil
}

// readHeaderInt reads the rest of a "*<n>", "$<n>" or ":<n>" line (the type
// byte was already consumed). It requires CRLF and a canonical integer.
func (r *Reader) readHeaderInt(badMsg string) (int64, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			return 0, protoErr(badMsg)
		}
		return 0, eofToUnexpected(err)
	}
	if len(line) < 3 || line[len(line)-2] != '\r' || len(line)-2 > maxHeaderLen {
		return 0, protoErr(badMsg)
	}
	n, ok := parseInt(line[:len(line)-2])
	if !ok {
		return 0, protoErr(badMsg)
	}
	return n, nil
}

func (r *Reader) readMultibulk() ([][]byte, error) {
	n, err := r.readHeaderInt("invalid multibulk length")
	if err != nil {
		return nil, err
	}
	if n > MaxMultibulkLen {
		return nil, protoErr("invalid multibulk length")
	}
	if n <= 0 {
		return nil, nil
	}
	args := make([][]byte, 0, min(int(n), 64)) // never trust the announced count for allocation
	for i := int64(0); i < n; i++ {
		b, err := r.br.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != '$' {
			return nil, protoErr(fmt.Sprintf("expected '$', got '%c'", b))
		}
		l, err := r.readHeaderInt("invalid bulk length")
		if err != nil {
			return nil, err
		}
		if l < 0 || l > MaxBulkLen {
			return nil, protoErr("invalid bulk length")
		}
		data, err := r.readBulkBody(int(l))
		if err != nil {
			return nil, err
		}
		args = append(args, data)
	}
	return args, nil
}

// readBulkBody reads n bytes of payload plus the trailing CRLF.
func (r *Reader) readBulkBody(n int) ([]byte, error) {
	data, err := r.readFull(n)
	if err != nil {
		return nil, err
	}
	var crlf [2]byte
	if _, err := io.ReadFull(r.br, crlf[:]); err != nil {
		return nil, eofToUnexpected(err)
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return nil, protoErr("expected CRLF after bulk string")
	}
	return data, nil
}

// readFull reads exactly n bytes. Big reads grow the buffer as data actually
// arrives, so a client announcing a 512MB bulk and sending 10 bytes costs us
// about 64KB, not 512MB.
func (r *Reader) readFull(n int) ([]byte, error) {
	if n <= readChunk {
		buf := make([]byte, n)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return nil, eofToUnexpected(err)
		}
		return buf, nil
	}
	buf := make([]byte, 0, readChunk)
	for len(buf) < n {
		step := min(n-len(buf), readChunk)
		buf = slices.Grow(buf, step)
		cur := len(buf)
		buf = buf[:cur+step]
		if _, err := io.ReadFull(r.br, buf[cur:]); err != nil {
			return nil, eofToUnexpected(err)
		}
	}
	return buf, nil
}

// ReadValue reads one RESP2 reply of any type. It is the client side of the
// protocol; the server only needs ReadCommand. Used by tests, tooling and the
// benchmark client.
func (r *Reader) ReadValue() (Value, error) { return r.readValue(0) }

func (r *Reader) readValue(depth int) (Value, error) {
	if depth > maxDepth {
		return Value{}, protoErr("reply nesting too deep")
	}
	t, err := r.br.ReadByte()
	if err != nil {
		return Value{}, err
	}
	v, err := r.readValueBody(Kind(t), depth)
	if err != nil {
		return Value{}, eofToUnexpected(err)
	}
	return v, nil
}

func clone(b []byte) []byte { return append(make([]byte, 0, len(b)), b...) }

func (r *Reader) readValueBody(k Kind, depth int) (Value, error) {
	switch k {
	case SimpleString, Error:
		line, crlf, err := r.readLine(MaxInlineLen)
		if err != nil {
			if errors.Is(err, errLineTooLong) {
				return Value{}, protoErr("line too long")
			}
			return Value{}, err
		}
		if !crlf {
			return Value{}, protoErr("expected CRLF")
		}
		// The spec forbids CR and LF inside these types, and Writer.oneLine would
		// rewrite a stray CR: accepting it here made "+\r\r\n" parse to a value
		// that did not survive a write/read round trip (found by FuzzReadValue).
		if bytes.IndexByte(line, '\r') >= 0 {
			return Value{}, protoErr("simple string contains CR")
		}
		return Value{Kind: k, Str: clone(line)}, nil
	case Integer:
		n, err := r.readHeaderInt("invalid integer")
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: Integer, Int: n}, nil
	case Bulk:
		n, err := r.readHeaderInt("invalid bulk length")
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return Value{Kind: Bulk, Null: true}, nil
		}
		if n < 0 || n > MaxBulkLen {
			return Value{}, protoErr("invalid bulk length")
		}
		data, err := r.readBulkBody(int(n))
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: Bulk, Str: data}, nil
	case Array:
		n, err := r.readHeaderInt("invalid multibulk length")
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return Value{Kind: Array, Null: true}, nil
		}
		if n < 0 || n > MaxMultibulkLen {
			return Value{}, protoErr("invalid multibulk length")
		}
		arr := make([]Value, 0, min(int(n), 64))
		for i := int64(0); i < n; i++ {
			v, err := r.readValue(depth + 1)
			if err != nil {
				return Value{}, err
			}
			arr = append(arr, v)
		}
		return Value{Kind: Array, Array: arr}, nil
	default:
		return Value{}, protoErr(fmt.Sprintf("unknown type byte '%c'", byte(k)))
	}
}

// parseInt parses a canonical base-10 int64 without allocating.
func parseInt(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	neg := false
	if b[0] == '-' {
		neg = true
		b = b[1:]
		if len(b) == 0 {
			return 0, false
		}
	}
	if len(b) > 19 { // 19 digits always fit in a uint64
		return 0, false
	}
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint64(c-'0')
	}
	if neg {
		if n > 1<<63 {
			return 0, false
		}
		return -int64(n), true
	}
	if n > math.MaxInt64 {
		return 0, false
	}
	return int64(n), true
}

// SplitInline splits an inline command line into arguments using the same
// rules as Redis' sdssplitargs: whitespace separates arguments; "double quoted"
// arguments understand \n \r \t \b \a \xHH and \\ \"; 'single quoted' ones
// only understand \'. A closing quote must be followed by whitespace or the
// end of the line, otherwise the line has unbalanced quotes.
func SplitInline(line []byte) ([][]byte, error) {
	var args [][]byte
	unbalanced := protoErr("unbalanced quotes in request")
	n := len(line)
	i := 0
	for {
		for i < n && isSpace(line[i]) {
			i++
		}
		if i >= n {
			return args, nil
		}
		cur := make([]byte, 0, 16)
		inDouble, inSingle, done := false, false, false
		for !done {
			switch {
			case inDouble:
				if i >= n {
					return nil, unbalanced
				}
				c := line[i]
				switch {
				case c == '\\' && i+3 < n && line[i+1] == 'x' && isHex(line[i+2]) && isHex(line[i+3]):
					cur = append(cur, hexVal(line[i+2])<<4|hexVal(line[i+3]))
					i += 3
				case c == '\\' && i+1 < n:
					i++
					switch line[i] {
					case 'n':
						cur = append(cur, '\n')
					case 'r':
						cur = append(cur, '\r')
					case 't':
						cur = append(cur, '\t')
					case 'b':
						cur = append(cur, '\b')
					case 'a':
						cur = append(cur, '\a')
					default:
						cur = append(cur, line[i])
					}
				case c == '"':
					if i+1 < n && !isSpace(line[i+1]) {
						return nil, unbalanced
					}
					done = true
				default:
					cur = append(cur, c)
				}
			case inSingle:
				if i >= n {
					return nil, unbalanced
				}
				c := line[i]
				switch {
				case c == '\\' && i+1 < n && line[i+1] == '\'':
					i++
					cur = append(cur, '\'')
				case c == '\'':
					if i+1 < n && !isSpace(line[i+1]) {
						return nil, unbalanced
					}
					done = true
				default:
					cur = append(cur, c)
				}
			default:
				if i >= n {
					done = true
					break
				}
				switch c := line[i]; {
				case isSpace(c) || c == 0:
					done = true
				case c == '"':
					inDouble = true
				case c == '\'':
					inSingle = true
				default:
					cur = append(cur, c)
				}
			}
			if i < n {
				i++
			}
		}
		args = append(args, cur)
	}
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	default:
		return c - 'A' + 10
	}
}
