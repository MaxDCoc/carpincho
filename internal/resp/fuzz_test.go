package resp

import (
	"bytes"
	"testing"
)

func equalValue(a, b Value) bool {
	if a.Kind != b.Kind || a.Null != b.Null || a.Int != b.Int || !bytes.Equal(a.Str, b.Str) || len(a.Array) != len(b.Array) {
		return false
	}
	for i := range a.Array {
		if !equalValue(a.Array[i], b.Array[i]) {
			return false
		}
	}
	return true
}

func equalArgs(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// FuzzReadCommand feeds arbitrary bytes to the request parser. Properties:
//  1. it never panics, hangs or returns empty argument lists;
//  2. whatever it accepts can be re-encoded as a multibulk request and parsed
//     back to exactly the same arguments.
func FuzzReadCommand(f *testing.F) {
	seeds := []string{
		"*1\r\n$4\r\nPING\r\n",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n",
		"PING\r\n",
		"SET k \"a b\\x41\"\r\n",
		"SET k 'it\\'s'\r\n",
		"*0\r\n",
		"*-1\r\n",
		"$-1\r\n",
		"\r\n",
		"*1\r\n$0\r\n\r\n",
		"*2\r\n$3\r\nGET\r\n$536870912\r\n",
		"*1\r\n$3\r\nabcXY",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for i := 0; i < 10_000; i++ {
			args, err := r.ReadCommand()
			if err != nil {
				return
			}
			if len(args) == 0 {
				t.Fatalf("ReadCommand returned no args and no error for %q", data)
			}
			enc := AppendCommand(nil, args)
			back, err := NewReader(bytes.NewReader(enc)).ReadCommand()
			if err != nil {
				t.Fatalf("re-parse of %q failed: %v", enc, err)
			}
			if !equalArgs(args, back) {
				t.Fatalf("roundtrip mismatch: %q != %q", args, back)
			}
		}
	})
}

// FuzzReadValue does the same for the reply parser (client side of the protocol).
func FuzzReadValue(f *testing.F) {
	seeds := []string{
		"+OK\r\n", "-ERR x\r\n", ":123\r\n", "$3\r\nfoo\r\n", "$-1\r\n", "*-1\r\n",
		"*2\r\n$1\r\na\r\n:5\r\n", "*1\r\n*1\r\n*1\r\n:1\r\n", "$0\r\n\r\n", ":-0\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := NewReader(bytes.NewReader(data)).ReadValue()
		if err != nil {
			return
		}
		var buf bytes.Buffer
		w := NewWriter(&buf)
		w.Value(v)
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		back, err := NewReader(&buf).ReadValue()
		if err != nil {
			t.Fatalf("re-read failed: %v", err)
		}
		if !equalValue(v, back) {
			t.Fatalf("roundtrip mismatch: %s != %s", v, back)
		}
	})
}

func FuzzSplitInline(f *testing.F) {
	for _, s := range []string{`a b c`, `"a b"`, `'x y'`, `"\x41"`, `"`, `'`, `\`, `"\`, "a\x00b"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		args, err := SplitInline(data)
		if err != nil {
			return
		}
		for _, a := range args {
			if a == nil {
				t.Fatal("nil argument")
			}
		}
	})
}
