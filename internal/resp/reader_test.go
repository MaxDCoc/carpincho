package resp

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"testing/iotest"
)

func strs(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}

func TestReadCommandOK(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"multibulk ping", "*1\r\n$4\r\nPING\r\n", []string{"PING"}},
		{"multibulk set", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n", []string{"SET", "k", "hello"}},
		{"binary safe bulk", "*2\r\n$4\r\nECHO\r\n$7\r\na\r\nb\x00\r\n\r\n", []string{"ECHO", "a\r\nb\x00\r\n"}},
		{"empty bulk", "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n", []string{"ECHO", ""}},
		{"inline", "PING\r\n", []string{"PING"}},
		{"inline bare LF", "PING\n", []string{"PING"}},
		{"inline args", "SET k v\r\n", []string{"SET", "k", "v"}},
		{"inline double quotes", "SET k \"hello world\"\r\n", []string{"SET", "k", "hello world"}},
		{"inline escapes", `SET k "a\tb\x41\n"` + "\r\n", []string{"SET", "k", "a\tbA\n"}},
		{"inline single quotes", `SET k 'it\'s'` + "\r\n", []string{"SET", "k", "it's"}},
		{"inline empty quoted arg", `SET k ""` + "\r\n", []string{"SET", "k", ""}},
		{"extra spaces", "  GET   k  \r\n", []string{"GET", "k"}},
		{"blank lines skipped", "\r\n\r\nPING\r\n", []string{"PING"}},
		{"empty multibulk skipped", "*0\r\n*-1\r\nPING\r\n", []string{"PING"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewReader(strings.NewReader(tt.in)).ReadCommand()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(strs(got), tt.want) {
				t.Fatalf("got %q, want %q", strs(got), tt.want)
			}
		})
	}
}

func TestReadCommandPipelined(t *testing.T) {
	in := "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"
	r := NewReader(strings.NewReader(in))
	want := [][]string{{"PING"}, {"GET", "k"}, {"PING"}, {"ECHO", "hi"}}
	for i, w := range want {
		got, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if !reflect.DeepEqual(strs(got), w) {
			t.Fatalf("command %d: got %q want %q", i, strs(got), w)
		}
	}
	if _, err := r.ReadCommand(); err != io.EOF {
		t.Fatalf("after last command want io.EOF, got %v", err)
	}
}

// A TCP connection delivers bytes in arbitrary chunks. The parser must give the
// same result when the input arrives one byte at a time.
func TestReadCommandStreamingOneByteAtATime(t *testing.T) {
	in := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\nPING\r\n"
	r := NewReader(iotest.OneByteReader(strings.NewReader(in)))
	a, err := r.ReadCommand()
	if err != nil || !reflect.DeepEqual(strs(a), []string{"SET", "k", "hello"}) {
		t.Fatalf("first: %q %v", strs(a), err)
	}
	b, err := r.ReadCommand()
	if err != nil || !reflect.DeepEqual(strs(b), []string{"PING"}) {
		t.Fatalf("second: %q %v", strs(b), err)
	}
}

func TestReadCommandLargeBulkAcrossBufferBoundary(t *testing.T) {
	payload := strings.Repeat("x", 200_000)
	in := "*2\r\n$4\r\nECHO\r\n$200000\r\n" + payload + "\r\n"
	got, err := NewReader(strings.NewReader(in)).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	if string(got[1]) != payload {
		t.Fatalf("payload mismatch (len %d)", len(got[1]))
	}
}

func TestReadCommandProtocolErrors(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"invalid multibulk length", "*abc\r\n", "invalid multibulk length"},
		{"too many args", "*1048577\r\n", "invalid multibulk length"},
		{"expected dollar", "*1\r\n+OK\r\n", "expected '$', got '+'"},
		{"negative bulk", "*1\r\n$-5\r\n", "invalid bulk length"},
		{"huge bulk", "*1\r\n$536870913\r\n", "invalid bulk length"},
		{"bad bulk number", "*1\r\n$x\r\n", "invalid bulk length"},
		{"missing CRLF after bulk", "*1\r\n$3\r\nabcXY", "expected CRLF after bulk string"},
		{"unbalanced double quotes", "SET k \"abc\r\n", "unbalanced quotes in request"},
		{"unbalanced single quotes", "SET k 'abc\r\n", "unbalanced quotes in request"},
		{"closing quote not followed by space", "SET k \"abc\"def\r\n", "unbalanced quotes in request"},
		{"inline too long", strings.Repeat("a", 70_000) + "\r\n", "too big inline request"},
		{"multibulk header with bare LF", "*1\n$4\r\nPING\r\n", "invalid multibulk length"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tt.in)).ReadCommand()
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("want *ProtocolError, got %T %v", err, err)
			}
			if pe.Msg != tt.want {
				t.Fatalf("got %q, want %q", pe.Msg, tt.want)
			}
		})
	}
}

func TestReadCommandTruncatedInputIsUnexpectedEOF(t *testing.T) {
	for _, in := range []string{"*", "*1", "*1\r\n", "*1\r\n$", "*1\r\n$4\r\n", "*1\r\n$4\r\nPI", "*1\r\n$4\r\nPING", "*1\r\n$4\r\nPING\r", "PIN", "*2\r\n$3\r\nGET\r\n"} {
		_, err := NewReader(strings.NewReader(in)).ReadCommand()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("%q: want io.ErrUnexpectedEOF, got %v", in, err)
		}
	}
	if _, err := NewReader(strings.NewReader("")).ReadCommand(); err != io.EOF {
		t.Fatalf("empty input: want io.EOF, got %v", err)
	}
}

// A lying bulk length must not make the server allocate the claimed size up front.
func TestReadCommandDoesNotPreallocateClaimedBulkSize(t *testing.T) {
	in := "*1\r\n$536870912\r\nonly a few bytes"
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := NewReader(strings.NewReader(in)).ReadCommand()
	runtime.ReadMemStats(&after)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want io.ErrUnexpectedEOF, got %v", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 8<<20 {
		t.Fatalf("parser allocated %d bytes for a 512MB claim with ~15 bytes of data", grew)
	}
}

func TestSplitInline(t *testing.T) {
	tests := []struct {
		in   string
		want []string
		bad  bool
	}{
		{"a b c", []string{"a", "b", "c"}, false},
		{`a "b c" d`, []string{"a", "b c", "d"}, false},
		{`a 'b c' d`, []string{"a", "b c", "d"}, false},
		{`"\x41\x42"`, []string{"AB"}, false},
		{`"tab\there"`, []string{"tab\there"}, false},
		{`"unterminated`, nil, true},
		{`'unterminated`, nil, true},
		{`"a"b`, nil, true},
		{"", nil, false},
	}
	for _, tt := range tests {
		got, err := SplitInline([]byte(tt.in))
		if tt.bad {
			if err == nil {
				t.Errorf("%q: want error", tt.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", tt.in, err)
			continue
		}
		if len(got) != len(tt.want) {
			t.Errorf("%q: got %q want %q", tt.in, strs(got), tt.want)
			continue
		}
		for i := range got {
			if string(got[i]) != tt.want[i] {
				t.Errorf("%q: got %q want %q", tt.in, strs(got), tt.want)
			}
		}
	}
}

func TestAppendCommandRoundTrip(t *testing.T) {
	args := [][]byte{[]byte("SET"), []byte("key"), []byte("va\r\nlue"), {}}
	enc := AppendCommand(nil, args)
	want := "*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nva\r\nlue\r\n$0\r\n\r\n"
	if string(enc) != want {
		t.Fatalf("got %q want %q", enc, want)
	}
	got, err := NewReader(bytes.NewReader(enc)).ReadCommand()
	if err != nil || !reflect.DeepEqual(strs(got), strs(args)) {
		t.Fatalf("roundtrip: %q %v", strs(got), err)
	}
}
