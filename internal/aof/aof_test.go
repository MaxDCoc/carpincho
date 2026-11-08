package aof

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/MaxDCoc/carpincho/internal/resp"
)

func rec(args ...string) []byte { return resp.AppendCommandStrings(nil, args...) }

type recorder struct{ cmds []string }

func (r *recorder) apply(args [][]byte) error {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = string(a)
	}
	r.cmds = append(r.cmds, strings.Join(parts, " "))
	return nil
}

func openT(t *testing.T, dir string, p Policy) (*AOF, *recorder, LoadStats) {
	t.Helper()
	r := &recorder{}
	a, st, err := Open(dir, p, r.apply)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Windows cannot delete a directory that holds an open file, and Close is
	// idempotent, so every opened log is closed when the test ends.
	t.Cleanup(func() { _ = a.Close() })
	return a, r, st
}

func appendCommit(t *testing.T, a *AOF, args ...string) {
	t.Helper()
	if err := a.Commit(a.Append(rec(args...))); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func eq(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestAppendedCommandsAreReplayedInOrderAfterReopen(t *testing.T) {
	for _, p := range []Policy{Always, Everysec, No} {
		t.Run(p.String(), func(t *testing.T) {
			dir := t.TempDir()
			a, r, st := openT(t, dir, p)
			if len(r.cmds) != 0 || st.Commands != 0 {
				t.Fatal("a new directory must replay nothing")
			}
			appendCommit(t, a, "SET", "a", "1")
			appendCommit(t, a, "RPUSH", "l", "x", "y z")
			appendCommit(t, a, "DEL", "a")
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}

			_, r2, st2 := openT(t, dir, p)
			eq(t, r2.cmds, "SET a 1", "RPUSH l x y z", "DEL a")
			if st2.Commands != 3 || st2.Truncated != 0 {
				t.Fatalf("stats = %+v", st2)
			}
		})
	}
}

func TestOpenToleratesATruncatedTailAndKeepsAppending(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)
	good := string(rec("SET", "a", "1")) + string(rec("SET", "b", "2"))
	torn := string(rec("SET", "c", "3"))
	torn = torn[:len(torn)-4] // the process died in the middle of writing this record
	if err := os.WriteFile(path, []byte(good+torn), 0o644); err != nil {
		t.Fatal(err)
	}

	a, r, st := openT(t, dir, Everysec)
	eq(t, r.cmds, "SET a 1", "SET b 2")
	if st.Truncated != int64(len(torn)) {
		t.Fatalf("Truncated = %d, want %d", st.Truncated, len(torn))
	}
	if fi, _ := os.Stat(path); fi.Size() != int64(len(good)) {
		t.Fatalf("file must be cut back to the last complete record: size %d, want %d", fi.Size(), len(good))
	}

	appendCommit(t, a, "SET", "d", "4")
	_ = a.Close()
	_, r2, _ := openT(t, dir, Everysec)
	eq(t, r2.cmds, "SET a 1", "SET b 2", "SET d 4")
}

func TestOpenRefusesACorruptFileInsteadOfSilentlyLosingData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)
	content := string(rec("SET", "a", "1")) + "*2\r\n$3\r\nSET\r\n+oops\r\n" + string(rec("SET", "b", "2"))
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(dir, Everysec, (&recorder{}).apply); err == nil || !strings.Contains(err.Error(), "corrupted") {
		t.Fatalf("want a corruption error, got %v", err)
	}
	if b, _ := os.ReadFile(path); string(b) != content {
		t.Fatal("a corrupt file must be left untouched for the operator to inspect")
	}
}

func TestRewriteReplacesTheLogAndKeepsAppending(t *testing.T) {
	dir := t.TempDir()
	a, _, _ := openT(t, dir, Always)
	for i := 0; i < 5; i++ {
		appendCommit(t, a, "INCR", "counter")
	}

	rw, err := a.BeginRewrite()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = rw.Write(rec("SET", "counter", "5"))
	_, _ = rw.Write(rec("SET", "other", "x"))
	if err := rw.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, FileName+".rewrite.tmp")); !os.IsNotExist(err) {
		t.Fatal("the temporary file must be gone after the swap")
	}

	appendCommit(t, a, "INCR", "counter")
	_ = a.Close()

	_, r, _ := openT(t, dir, Always)
	eq(t, r.cmds, "SET counter 5", "SET other x", "INCR counter")
}

func TestRewriteAbortLeavesTheOldLogAlone(t *testing.T) {
	dir := t.TempDir()
	a, _, _ := openT(t, dir, Always)
	appendCommit(t, a, "SET", "a", "1")
	rw, err := a.BeginRewrite()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = rw.Write(rec("SET", "garbage", "x"))
	rw.Abort()
	appendCommit(t, a, "SET", "b", "2")
	_ = a.Close()
	_, r, _ := openT(t, dir, Always)
	eq(t, r.cmds, "SET a 1", "SET b 2")
}

func TestConcurrentAppendersAllReachTheDiskWithGroupCommit(t *testing.T) {
	dir := t.TempDir()
	a, _, _ := openT(t, dir, Always)
	const workers, each = 8, 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				seq := a.Append(rec("SET", "k"+strconv.Itoa(w)+"-"+strconv.Itoa(i), "v"))
				if err := a.Commit(seq); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	_ = a.Close()
	_, r, _ := openT(t, dir, Always)
	if len(r.cmds) != workers*each {
		t.Fatalf("replayed %d commands, want %d", len(r.cmds), workers*each)
	}
}

func TestParsePolicy(t *testing.T) {
	for in, want := range map[string]Policy{"always": Always, "EVERYSEC": Everysec, "no": No} {
		if got, err := ParsePolicy(in); err != nil || got != want {
			t.Errorf("ParsePolicy(%q) = %v %v", in, got, err)
		}
	}
	if _, err := ParsePolicy("sometimes"); err == nil {
		t.Fatal("want an error for an unknown policy")
	}
}

func TestCommitAfterCloseFails(t *testing.T) {
	a, _, _ := openT(t, t.TempDir(), No)
	seq := a.Append(rec("SET", "a", "1"))
	_ = a.Close()
	if err := a.Commit(seq); err == nil {
		t.Fatal("Commit on a closed AOF must fail")
	}
}
