package server

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MaxDCoc/carpincho/internal/resp"
)

type fakeClock struct{ ms atomic.Int64 }

func (c *fakeClock) now() int64              { return c.ms.Load() }
func (c *fakeClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

// harness runs commands against a Server without any network: the reply bytes
// are decoded back with resp.ReadValue and rendered with Value.String(), so a
// test reads like a redis-cli session.
type harness struct {
	t     *testing.T
	srv   *Server
	cl    *Client
	buf   *bytes.Buffer
	clock *fakeClock
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	srv, err := New(Config{Clock: clk.now})
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, srv: srv, buf: &bytes.Buffer{}, clock: clk}
	h.cl = newClient(srv, nil)
	h.cl.w = resp.NewWriter(h.buf)
	t.Cleanup(func() {
		h.cl.close()
		_ = srv.Shutdown(context.Background())
	})
	return h
}

func (h *harness) do(args ...string) string {
	h.t.Helper()
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	h.buf.Reset()
	h.cl.wmu.Lock()
	h.srv.execute(h.cl, bs)
	h.cl.wmu.Unlock()
	h.cl.flushPending() // same path as the connection loop: AOF commit first, then the reply
	rd := resp.NewReader(bytes.NewReader(h.buf.Bytes()))
	var parts []string
	for {
		v, err := rd.ReadValue()
		if err == io.EOF {
			break
		}
		if err != nil {
			h.t.Fatalf("%v: reply is not valid RESP: %v (%q)", args, err, h.buf.Bytes())
		}
		parts = append(parts, v.String())
	}
	return strings.Join(parts, " ; ")
}

// run takes alternating "command line" / "expected reply" strings.
func (h *harness) run(steps ...string) {
	h.t.Helper()
	for i := 0; i+1 < len(steps); i += 2 {
		if got := h.do(strings.Fields(steps[i])...); got != steps[i+1] {
			h.t.Errorf("%s\n  got  %s\n  want %s", steps[i], got, steps[i+1])
		}
	}
}

// sortedArray runs a command whose array reply has no defined order.
func (h *harness) sortedArray(args ...string) string {
	h.t.Helper()
	h.buf.Reset()
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	h.cl.wmu.Lock()
	h.srv.execute(h.cl, bs)
	_ = h.cl.w.Flush()
	h.cl.wmu.Unlock()
	v, err := resp.NewReader(bytes.NewReader(h.buf.Bytes())).ReadValue()
	if err != nil || v.Kind != resp.Array {
		h.t.Fatalf("%v: not an array: %v %s", args, err, v)
	}
	items := make([]string, len(v.Array))
	for i, e := range v.Array {
		items[i] = string(e.Str)
	}
	sort.Strings(items)
	return strings.Join(items, ",")
}

const wrongTypeReply = "-WRONGTYPE Operation against a key holding the wrong kind of value"

func TestStringCommands(t *testing.T) {
	h := newHarness(t)
	h.run(
		"SET k v", "+OK",
		"GET k", `"v"`,
		"GET missing", "(nil)",
		"SET k v2 NX", "(nil)",
		"SET fresh v NX", "+OK",
		"SET nope v XX", "(nil)",
		"SET k v3 XX", "+OK",
		"GET k", `"v3"`,
		"set k v4", "+OK", // command names are case-insensitive
		"GeT k", `"v4"`,
		"MSET a 1 b 2", "+OK",
		"MGET a b c", `["1", "2", (nil)]`,
		"INCR n", ":1",
		"INCR n", ":2",
		"INCRBY n 10", ":12",
		"DECR n", ":11",
		"DECRBY n 11", ":0",
		"INCRBY n abc", "-ERR value is not an integer or out of range",
		"SET s abc", "+OK",
		"INCR s", "-ERR value is not an integer or out of range",
		"SET padded 007", "+OK",
		"INCR padded", "-ERR value is not an integer or out of range",
		"SET big 9223372036854775807", "+OK",
		"INCR big", "-ERR increment or decrement would overflow",
		"DECRBY n -9223372036854775808", "-ERR increment or decrement would overflow",
		"APPEND ap hello", ":5",
		"APPEND ap !!", ":7",
		"GET ap", `"hello!!"`,
		"STRLEN ap", ":7",
		"STRLEN nokey", ":0",
	)
}

func TestSetOptionsAndErrors(t *testing.T) {
	h := newHarness(t)
	h.run(
		"SET k v EX 10", "+OK",
		"TTL k", ":10",
		"PTTL k", ":10000",
		"SET k v4 KEEPTTL", "+OK",
		"TTL k", ":10",
		"SET k v", "+OK",
		"TTL k", ":-1",
		"SET k v PX 1500", "+OK",
		"PTTL k", ":1500",
		"TTL k", ":2", // TTL rounds to the nearest second like Redis
		"SET k v NX XX", "-ERR syntax error",
		"SET k v EX 0", "-ERR invalid expire time in 'set' command",
		"SET k v EX -5", "-ERR invalid expire time in 'set' command",
		"SET k v EX 9223372036854775807", "-ERR invalid expire time in 'set' command",
		"SET k v EX abc", "-ERR value is not an integer or out of range",
		"SET k v EX 10 PX 100", "-ERR syntax error",
		"SET k v KEEPTTL EX 10", "-ERR syntax error",
		"SET k v EX", "-ERR syntax error",
		"SET k v FOO", "-ERR syntax error",
		"MSET a 1 b", "-ERR wrong number of arguments for 'mset' command",
	)
}

func TestWrongTypeEverywhere(t *testing.T) {
	cases := []struct{ setup, cmd string }{
		{"LPUSH k a", "GET k"},
		{"LPUSH k a", "INCR k"},
		{"LPUSH k a", "APPEND k x"},
		{"LPUSH k a", "STRLEN k"},
		{"LPUSH k a", "HSET k f v"},
		{"LPUSH k a", "HGET k f"},
		{"LPUSH k a", "SADD k m"},
		{"LPUSH k a", "SCARD k"},
		{"SET k v", "LPUSH k a"},
		{"SET k v", "RPOP k"},
		{"SET k v", "LLEN k"},
		{"SET k v", "LRANGE k 0 -1"},
		{"SET k v", "HGETALL k"},
		{"SET k v", "HLEN k"},
		{"SET k v", "SMEMBERS k"},
		{"SET k v", "SISMEMBER k m"},
		{"HSET k f v", "LPUSH k a"},
		{"SADD k m", "HDEL k f"},
	}
	for _, tc := range cases {
		h := newHarness(t)
		h.do(strings.Fields(tc.setup)...)
		if got := h.do(strings.Fields(tc.cmd)...); got != wrongTypeReply {
			t.Errorf("%s then %s: got %s", tc.setup, tc.cmd, got)
		}
	}
	// SET overwrites regardless of the previous type, and MGET answers nil for non-strings.
	h := newHarness(t)
	h.run("LPUSH k a", ":1", "MGET k", "[(nil)]", "SET k v", "+OK", "GET k", `"v"`)
}

func TestKeyspaceCommands(t *testing.T) {
	h := newHarness(t)
	h.run(
		"SET a 1", "+OK", "SET b 2", "+OK",
		"EXISTS a b nope a", ":3", // duplicates are counted
		"DEL a nope", ":1",
		"TYPE b", "+string", "TYPE nope", "+none",
		"LPUSH l x", ":1", "TYPE l", "+list",
		"HSET h f v", ":1", "TYPE h", "+hash",
		"SADD s m", ":1", "TYPE s", "+set",
		"DBSIZE", ":4",
		"EXPIRE b 100", ":1", "TTL b", ":100",
		"EXPIRE nope 100", ":0",
		"PERSIST b", ":1", "PERSIST b", ":0", "TTL b", ":-1", "TTL nope", ":-2", "PTTL nope", ":-2",
		"EXPIRE b abc", "-ERR value is not an integer or out of range",
		"EXPIRE b 9223372036854775807", "-ERR invalid expire time in 'expire' command",
		"PEXPIRE b 1500", ":1", "PTTL b", ":1500",
		"EXPIRE b -1", ":1", // a time in the past deletes the key
		"GET b", "(nil)",
		"SET x 1", "+OK",
		"FLUSHALL", "+OK", "DBSIZE", ":0",
		"GET", "-ERR wrong number of arguments for 'get' command",
		"GET a b", "-ERR wrong number of arguments for 'get' command",
		"NOSUCH x y", "-ERR unknown command 'NOSUCH', with args beginning with: 'x' 'y' ",
	)
	h.run("SET user:1 a", "+OK", "SET user:2 b", "+OK", "SET other c", "+OK")
	if got := h.sortedArray("KEYS", "user:*"); got != "user:1,user:2" {
		t.Errorf("KEYS user:* = %s", got)
	}
	if got := h.sortedArray("KEYS", "*"); got != "other,user:1,user:2" {
		t.Errorf("KEYS * = %s", got)
	}
}

func TestExpiryThroughCommands(t *testing.T) {
	h := newHarness(t)
	h.run("SET a 1 PX 100", "+OK", "SET b 2", "+OK")
	h.clock.advance(99 * time.Millisecond)
	h.run("GET a", `"1"`)
	h.clock.advance(1 * time.Millisecond)
	h.run("GET a", "(nil)", "EXISTS a", ":0", "TTL a", ":-2", "TYPE a", "+none")

	// Keys nobody reads again are collected by the active cycle.
	for i := 0; i < 200; i++ {
		h.do("SET", "tmp:"+string(rune('a'+i%26))+strings.Repeat("x", i), "v", "EX", "1")
	}
	h.run("DBSIZE", ":202") // 200 temporary keys + b + the expired "a", which nobody has collected yet (Redis counts it too)
	h.clock.advance(2 * time.Second)
	h.srv.store.ExpireCycle(time.Second)
	h.run("DBSIZE", ":1")
}

func TestListCommands(t *testing.T) {
	h := newHarness(t)
	h.run(
		"LPUSH l a b c", ":3", // pushes one by one at the head: c b a
		"LRANGE l 0 -1", `["c", "b", "a"]`,
		"RPUSH l x", ":4",
		"LLEN l", ":4",
		"LPOP l", `"c"`,
		"RPOP l", `"x"`,
		"LPOP l 2", `["b", "a"]`,
		"EXISTS l", ":0", // an emptied list disappears
		"LPOP l", "(nil)",
		"LPOP l 2", "(nil array)",
		"RPUSH l 1 2 3 4 5", ":5",
		"LRANGE l -2 -1", `["4", "5"]`,
		"LRANGE l 10 20", "[]",
		"LRANGE nokey 0 -1", "[]",
		"LRANGE l a b", "-ERR value is not an integer or out of range",
		"LPOP l -1", "-ERR value is out of range, must be positive",
		"LPOP l 0", "[]",
		"LLEN nokey", ":0",
		"RPOP l 10", `["5", "4", "3", "2", "1"]`,
	)
}

func TestHashCommands(t *testing.T) {
	h := newHarness(t)
	h.run(
		"HSET h a 1 b 2", ":2",
		"HSET h a 9 c 3", ":1", // only new fields are counted
		"HGET h a", `"9"`,
		"HGET h zz", "(nil)",
		"HGET nokey a", "(nil)",
		"HLEN h", ":3",
		"HDEL h a zz", ":1",
		"HSET h a", "-ERR wrong number of arguments for 'hset' command",
		"HLEN nokey", ":0",
	)
	if got := h.sortedArray("HGETALL", "h"); got != "2,3,b,c" {
		t.Errorf("HGETALL = %s", got)
	}
	h.run("HDEL h b c", ":2", "EXISTS h", ":0", "HGETALL h", "[]")
}

func TestSetCommands(t *testing.T) {
	h := newHarness(t)
	h.run(
		"SADD s a b a", ":2",
		"SADD s a", ":0",
		"SCARD s", ":2",
		"SISMEMBER s a", ":1",
		"SISMEMBER s z", ":0",
		"SREM s a z", ":1",
		"SREM s b", ":1",
		"EXISTS s", ":0",
		"SMEMBERS nokey", "[]",
		"SCARD nokey", ":0",
	)
	h.do("SADD", "s2", "x", "y", "z")
	if got := h.sortedArray("SMEMBERS", "s2"); got != "x,y,z" {
		t.Errorf("SMEMBERS = %s", got)
	}
}

func TestConnectionAndServerCommands(t *testing.T) {
	h := newHarness(t)
	h.run(
		"PING", "+PONG",
		"PING hi", `"hi"`,
		"ECHO hey", `"hey"`,
		"ECHO", "-ERR wrong number of arguments for 'echo' command",
		"PING a b", "-ERR wrong number of arguments for 'ping' command",
		"CLIENT SETNAME app", "+OK",
		"CLIENT GETNAME", `"app"`,
		"CLIENT SETINFO LIB-NAME go-redis", "+OK",
		"CLIENT SETNAME a_b", "+OK",
		"CLIENT SETINFO LIB-VER 9", "+OK",
		"CLIENT BOGUS", "-ERR unknown subcommand 'BOGUS'. Try CLIENT HELP.",
		"HELLO 3", "-NOPROTO sorry, this protocol version is not supported",
		"HELLO 9", "-NOPROTO unsupported protocol version",
		"HELLO abc", "-ERR Protocol version is not an integer or out of range",
		"SELECT 0", "+OK",
		"SELECT 1", "-ERR DB index is out of range",
		"COMMAND", "[]",
		"COMMAND DOCS", "[]",
		"CONFIG GET appendonly", `["appendonly", "no"]`,
		"CONFIG GET nothing-like-this", "[]",
		"BGREWRITEAOF", "-ERR AOF is not enabled: start carpincho with --dir to persist data",
	)
	if got := h.do("CLIENT", "SETNAME", "has space"); !strings.HasPrefix(got, "-ERR Client names cannot contain spaces") {
		t.Errorf("CLIENT SETNAME with a space: %s", got)
	}
	if got := h.do("CLIENT", "ID"); !strings.HasPrefix(got, ":") {
		t.Errorf("CLIENT ID: %s", got)
	}
	if got := h.do("HELLO", "2", "SETNAME", "x"); !strings.Contains(got, `"proto", :2`) || !strings.Contains(got, `"mode", "standalone"`) {
		t.Errorf("HELLO 2 = %s", got)
	}
	if got := h.do("COMMAND", "COUNT"); !strings.HasPrefix(got, ":") || got == ":0" {
		t.Errorf("COMMAND COUNT: %s", got)
	}
	if got := h.do("INFO"); !strings.Contains(got, "# Keyspace") || !strings.Contains(got, "carpincho_version") {
		t.Errorf("INFO: %s", got)
	}
	if got := h.do("INFO", "server"); strings.Contains(got, "# Keyspace") || !strings.Contains(got, "# Server") {
		t.Errorf("INFO server must return only its section: %s", got)
	}
	h.run("QUIT", "+OK")
	if !h.cl.quit {
		t.Error("QUIT must mark the client to be closed")
	}
}

func TestSubscribedClientsAreRestricted(t *testing.T) {
	h := newHarness(t)
	h.run(
		"SUBSCRIBE a b", `["subscribe", "a", :1] ; ["subscribe", "b", :2]`,
		"GET x", "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
		"PING", `["pong", ""]`,
		"UNSUBSCRIBE a", `["unsubscribe", "a", :1]`,
		"UNSUBSCRIBE", `["unsubscribe", "b", :0]`,
		"UNSUBSCRIBE", `["unsubscribe", (nil), :0]`,
		"GET x", "(nil)", // back to normal mode
		"PUBLISH a hello", ":0",
	)
}
