package resp

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// Writer encodes RESP2 replies into an internal buffer. Nothing reaches the
// underlying io.Writer until Flush, which lets a server answer a whole
// pipeline of commands with a single write syscall.
//
// Write errors are sticky (they come from bufio.Writer): the first one is
// returned by Flush, so the individual methods do not return errors.
type Writer struct {
	bw  *bufio.Writer
	num [24]byte
}

// NewWriter returns a Writer that buffers writes to w.
func NewWriter(w io.Writer) *Writer { return &Writer{bw: bufio.NewWriterSize(w, bufSize)} }

// Flush writes any buffered data to the underlying writer.
func (w *Writer) Flush() error { return w.bw.Flush() }

// Buffered returns the number of bytes waiting for Flush.
func (w *Writer) Buffered() int { return w.bw.Buffered() }

// oneLine makes a string safe to embed in a simple string or error reply: a
// CR or LF inside would end the line early and desynchronise the client.
func oneLine(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}

func (w *Writer) crlf() { _, _ = w.bw.WriteString("\r\n") }

func (w *Writer) number(prefix byte, n int64) {
	_ = w.bw.WriteByte(prefix)
	_, _ = w.bw.Write(strconv.AppendInt(w.num[:0], n, 10))
	w.crlf()
}

// SimpleString writes +s.
func (w *Writer) SimpleString(s string) {
	_ = w.bw.WriteByte('+')
	_, _ = w.bw.WriteString(oneLine(s))
	w.crlf()
}

// Error writes -msg. The message carries its own error code prefix, for
// example "ERR unknown command" or "WRONGTYPE Operation against a key ...".
func (w *Writer) Error(msg string) {
	_ = w.bw.WriteByte('-')
	_, _ = w.bw.WriteString(oneLine(msg))
	w.crlf()
}

// Int writes :n.
func (w *Writer) Int(n int64) { w.number(':', n) }

// Bulk writes a bulk string.
func (w *Writer) Bulk(b []byte) {
	w.number('$', int64(len(b)))
	_, _ = w.bw.Write(b)
	w.crlf()
}

// BulkString writes a bulk string from a Go string.
func (w *Writer) BulkString(s string) {
	w.number('$', int64(len(s)))
	_, _ = w.bw.WriteString(s)
	w.crlf()
}

// Null writes the null bulk string ($-1), the reply for "no such value".
func (w *Writer) Null() { _, _ = w.bw.WriteString("$-1\r\n") }

// NullArray writes the null array (*-1).
func (w *Writer) NullArray() { _, _ = w.bw.WriteString("*-1\r\n") }

// ArrayHeader starts an array of n elements; the caller writes the elements.
func (w *Writer) ArrayHeader(n int) { w.number('*', int64(n)) }

// BulkArray writes an array of bulk strings.
func (w *Writer) BulkArray(items [][]byte) {
	w.ArrayHeader(len(items))
	for _, it := range items {
		w.Bulk(it)
	}
}

// Raw appends bytes that are already RESP-encoded. Pub/sub encodes a message
// once and hands the same frame to every subscriber.
func (w *Writer) Raw(b []byte) { _, _ = w.bw.Write(b) }

// Value writes any decoded Value (used to echo what ReadValue parsed).
func (w *Writer) Value(v Value) {
	switch v.Kind {
	case SimpleString:
		w.SimpleString(string(v.Str))
	case Error:
		w.Error(string(v.Str))
	case Integer:
		w.Int(v.Int)
	case Bulk:
		if v.Null {
			w.Null()
			return
		}
		w.Bulk(v.Str)
	case Array:
		if v.Null {
			w.NullArray()
			return
		}
		w.ArrayHeader(len(v.Array))
		for _, e := range v.Array {
			w.Value(e)
		}
	}
}

// The Append* functions encode requests (what a client sends, and what the
// append-only file stores) into a caller-provided byte slice.

// AppendArrayHeader appends *n\r\n.
func AppendArrayHeader(dst []byte, n int) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, '\r', '\n')
}

// AppendBulk appends $len\r\nb\r\n.
func AppendBulk(dst, b []byte) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(b)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, b...)
	return append(dst, '\r', '\n')
}

// AppendBulkString is AppendBulk for a string.
func AppendBulkString(dst []byte, s string) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(s)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, s...)
	return append(dst, '\r', '\n')
}

// AppendCommand appends args encoded as a multibulk request.
func AppendCommand(dst []byte, args [][]byte) []byte {
	dst = AppendArrayHeader(dst, len(args))
	for _, a := range args {
		dst = AppendBulk(dst, a)
	}
	return dst
}

// AppendCommandStrings is AppendCommand for string arguments.
func AppendCommandStrings(dst []byte, args ...string) []byte {
	dst = AppendArrayHeader(dst, len(args))
	for _, a := range args {
		dst = AppendBulkString(dst, a)
	}
	return dst
}
