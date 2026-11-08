package resp

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestWriterEncodesEveryType(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SimpleString("OK")
	w.Error("ERR boom")
	w.Int(-42)
	w.Bulk([]byte("hi"))
	w.BulkString("")
	w.Null()
	w.NullArray()
	w.ArrayHeader(2)
	w.Int(1)
	w.BulkString("x")
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "+OK\r\n-ERR boom\r\n:-42\r\n$2\r\nhi\r\n$0\r\n\r\n$-1\r\n*-1\r\n*2\r\n:1\r\n$1\r\nx\r\n"
	if buf.String() != want {
		t.Fatalf("got  %q\nwant %q", buf.String(), want)
	}
}

func TestWriterNothingSentBeforeFlush(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SimpleString("OK")
	if buf.Len() != 0 {
		t.Fatalf("writer must buffer until Flush, but %d bytes were written", buf.Len())
	}
	if w.Buffered() == 0 {
		t.Fatal("Buffered() should report pending bytes")
	}
	_ = w.Flush()
	if buf.String() != "+OK\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestReadValueParsesEveryType(t *testing.T) {
	in := "+OK\r\n-ERR boom\r\n:-42\r\n$2\r\nhi\r\n$-1\r\n*-1\r\n*3\r\n:1\r\n$1\r\nx\r\n*0\r\n"
	r := NewReader(strings.NewReader(in))
	want := []Value{
		{Kind: SimpleString, Str: []byte("OK")},
		{Kind: Error, Str: []byte("ERR boom")},
		{Kind: Integer, Int: -42},
		{Kind: Bulk, Str: []byte("hi")},
		{Kind: Bulk, Null: true},
		{Kind: Array, Null: true},
		{Kind: Array, Array: []Value{{Kind: Integer, Int: 1}, {Kind: Bulk, Str: []byte("x")}, {Kind: Array, Array: []Value{}}}},
	}
	for i, w := range want {
		got, err := r.ReadValue()
		if err != nil {
			t.Fatalf("value %d: %v", i, err)
		}
		if !equalValue(got, w) {
			t.Fatalf("value %d: got %s want %s", i, got, w)
		}
	}
}

func TestReadValueRejectsGarbage(t *testing.T) {
	for _, in := range []string{"?\r\n", ":abc\r\n", "$-2\r\n", "*x\r\n", "+OK\n"} {
		if _, err := NewReader(strings.NewReader(in)).ReadValue(); err == nil {
			t.Errorf("%q: expected an error", in)
		}
	}
}

func TestReadValueRejectsAbsurdNesting(t *testing.T) {
	in := strings.Repeat("*1\r\n", 10_000) + ":1\r\n"
	if _, err := NewReader(strings.NewReader(in)).ReadValue(); err == nil {
		t.Fatal("expected a nesting error instead of a stack overflow")
	}
}

func TestWriterValueRoundTrip(t *testing.T) {
	v := Value{Kind: Array, Array: []Value{
		{Kind: SimpleString, Str: []byte("PONG")},
		{Kind: Bulk, Str: []byte("a\r\nb")},
		{Kind: Bulk, Null: true},
		{Kind: Integer, Int: 7},
		{Kind: Array, Array: []Value{{Kind: Error, Str: []byte("ERR x")}}},
	}}
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Value(v)
	_ = w.Flush()
	got, err := NewReader(&buf).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if !equalValue(got, v) {
		t.Fatalf("got %s want %s", got, v)
	}
	if !reflect.DeepEqual(got.Array[0].Str, []byte("PONG")) {
		t.Fatal("simple string content lost")
	}
}
