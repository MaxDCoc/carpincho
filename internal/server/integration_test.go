package server_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MaxDCoc/carpincho/internal/aof"
	"github.com/MaxDCoc/carpincho/internal/server"
)

var ctx = context.Background()

// running is a server listening on a real loopback TCP port, in this process.
type running struct {
	srv  *server.Server
	addr string
	errc chan error
	once sync.Once
}

func start(t *testing.T, cfg server.Config) *running {
	t.Helper()
	srv, err := server.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &running{srv: srv, addr: ln.Addr().String(), errc: make(chan error, 1)}
	go func() { r.errc <- srv.Serve(ln) }()
	t.Cleanup(func() { r.stop(t) })
	return r
}

// stop shuts the server down gracefully and checks that Serve returns. It is
// idempotent so tests can stop a server explicitly and still use t.Cleanup.
func (r *running) stop(t *testing.T) {
	t.Helper()
	r.once.Do(func() {
		sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := r.srv.Shutdown(sctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		select {
		case err := <-r.errc:
			if !errors.Is(err, server.ErrServerClosed) {
				t.Errorf("Serve returned %v, want ErrServerClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after Shutdown")
		}
	})
}

func dial(t *testing.T, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2, MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// check fails the test if the go-redis command returned an error, and hands the
// command back so the caller can read its value: check(t, rdb.Incr(...)).Val().
func check[C interface{ Err() error }](t *testing.T, c C) C {
	t.Helper()
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}
	return c
}

func getInt(t *testing.T, rdb *redis.Client, key string) int {
	t.Helper()
	n, err := rdb.Get(ctx, key).Int()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestGoRedisStringsAndKeys(t *testing.T) {
	rdb := dial(t, start(t, server.Config{}).addr)

	if err := rdb.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got := check(t, rdb.Get(ctx, "k")).Val(); got != "v" {
		t.Fatalf("Get = %q", got)
	}
	if _, err := rdb.Get(ctx, "missing").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("missing key must give redis.Nil, got %v", err)
	}
	if ok := check(t, rdb.SetNX(ctx, "k", "other", 0)).Val(); ok {
		t.Fatal("SetNX on an existing key must fail")
	}
	rdb.Set(ctx, "ttl", "x", 10*time.Second)
	if d := check(t, rdb.TTL(ctx, "ttl")).Val(); d <= 9*time.Second || d > 10*time.Second {
		t.Fatalf("TTL = %v", d)
	}
	if n := check(t, rdb.Incr(ctx, "n")).Val(); n != 1 {
		t.Fatalf("Incr = %d", n)
	}
	if n := check(t, rdb.IncrBy(ctx, "n", 41)).Val(); n != 42 {
		t.Fatalf("IncrBy = %d", n)
	}
	if n := check(t, rdb.Decr(ctx, "n")).Val(); n != 41 {
		t.Fatalf("Decr = %d", n)
	}
	if n := check(t, rdb.Append(ctx, "k", "123")).Val(); n != 4 {
		t.Fatalf("Append = %d", n)
	}
	if n := check(t, rdb.StrLen(ctx, "k")).Val(); n != 4 {
		t.Fatalf("StrLen = %d", n)
	}
	if err := rdb.MSet(ctx, "a", "1", "b", "2").Err(); err != nil {
		t.Fatal(err)
	}
	vals := check(t, rdb.MGet(ctx, "a", "b", "c")).Val()
	if len(vals) != 3 || vals[0] != "1" || vals[1] != "2" || vals[2] != nil {
		t.Fatalf("MGet = %v", vals)
	}
	if n := check(t, rdb.Exists(ctx, "a", "b", "c")).Val(); n != 2 {
		t.Fatalf("Exists = %d", n)
	}
	keys := check(t, rdb.Keys(ctx, "[ab]")).Val()
	sort.Strings(keys)
	if strings.Join(keys, ",") != "a,b" {
		t.Fatalf("Keys = %v", keys)
	}
	if n := check(t, rdb.Del(ctx, "a", "b", "nope")).Val(); n != 2 {
		t.Fatalf("Del = %d", n)
	}
	if typ := check(t, rdb.Type(ctx, "k")).Val(); typ != "string" {
		t.Fatalf("Type = %q", typ)
	}
	if ok := check(t, rdb.Expire(ctx, "k", time.Hour)).Val(); !ok {
		t.Fatal("Expire on an existing key must return true")
	}
	if ok := check(t, rdb.Persist(ctx, "k")).Val(); !ok {
		t.Fatal("Persist must remove the TTL")
	}
	if n := check(t, rdb.DBSize(ctx)).Val(); n != 3 { // k, ttl, n
		t.Fatalf("DBSize = %d", n)
	}
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if n := check(t, rdb.DBSize(ctx)).Val(); n != 0 {
		t.Fatalf("DBSize after FlushAll = %d", n)
	}
}

func TestGoRedisListHashSet(t *testing.T) {
	rdb := dial(t, start(t, server.Config{}).addr)

	check(t, rdb.LPush(ctx, "l", "a", "b", "c")).Val()
	check(t, rdb.RPush(ctx, "l", "x")).Val()
	if got := check(t, rdb.LRange(ctx, "l", 0, -1)).Val(); strings.Join(got, "") != "cbax" {
		t.Fatalf("LRange = %v", got)
	}
	if n := check(t, rdb.LLen(ctx, "l")).Val(); n != 4 {
		t.Fatalf("LLen = %d", n)
	}
	if v := check(t, rdb.LPop(ctx, "l")).Val(); v != "c" {
		t.Fatalf("LPop = %q", v)
	}
	if v := check(t, rdb.RPop(ctx, "l")).Val(); v != "x" {
		t.Fatalf("RPop = %q", v)
	}
	if got := check(t, rdb.LPopCount(ctx, "l", 5)).Val(); strings.Join(got, "") != "ba" {
		t.Fatalf("LPopCount = %v", got)
	}
	if _, err := rdb.LPop(ctx, "l").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("LPop on an emptied list: %v", err)
	}

	if n := check(t, rdb.HSet(ctx, "h", "f1", "v1", "f2", "v2")).Val(); n != 2 {
		t.Fatalf("HSet = %d", n)
	}
	all := check(t, rdb.HGetAll(ctx, "h")).Val()
	if len(all) != 2 || all["f1"] != "v1" || all["f2"] != "v2" {
		t.Fatalf("HGetAll = %v", all)
	}
	if v := check(t, rdb.HGet(ctx, "h", "f1")).Val(); v != "v1" {
		t.Fatalf("HGet = %q", v)
	}
	if n := check(t, rdb.HLen(ctx, "h")).Val(); n != 2 {
		t.Fatalf("HLen = %d", n)
	}
	if n := check(t, rdb.HDel(ctx, "h", "f1", "nope")).Val(); n != 1 {
		t.Fatalf("HDel = %d", n)
	}

	if n := check(t, rdb.SAdd(ctx, "s", "x", "y", "x")).Val(); n != 2 {
		t.Fatalf("SAdd = %d", n)
	}
	members := check(t, rdb.SMembers(ctx, "s")).Val()
	sort.Strings(members)
	if strings.Join(members, "") != "xy" {
		t.Fatalf("SMembers = %v", members)
	}
	if ok := check(t, rdb.SIsMember(ctx, "s", "x")).Val(); !ok {
		t.Fatal("SIsMember")
	}
	if n := check(t, rdb.SCard(ctx, "s")).Val(); n != 2 {
		t.Fatalf("SCard = %d", n)
	}
	if n := check(t, rdb.SRem(ctx, "s", "x", "zzz")).Val(); n != 1 {
		t.Fatalf("SRem = %d", n)
	}
}

func TestGoRedisErrorsMatchRedis(t *testing.T) {
	rdb := dial(t, start(t, server.Config{}).addr)
	rdb.Set(ctx, "str", "x", 0)
	err := rdb.LPush(ctx, "str", "a").Err()
	if err == nil || err.Error() != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("wrong type error = %v", err)
	}
	if err := rdb.Incr(ctx, "str").Err(); err == nil || err.Error() != "ERR value is not an integer or out of range" {
		t.Fatalf("incr error = %v", err)
	}
	if err := rdb.Do(ctx, "NOSUCHCMD", "a").Err(); err == nil || !strings.HasPrefix(err.Error(), "ERR unknown command 'NOSUCHCMD'") {
		t.Fatalf("unknown command error = %v", err)
	}
	if err := rdb.Do(ctx, "GET").Err(); err == nil || err.Error() != "ERR wrong number of arguments for 'get' command" {
		t.Fatalf("arity error = %v", err)
	}
}

// go-redis v9 speaks RESP3 by default and sends HELLO 3 first. carpincho
// answers -NOPROTO and the client must carry on with RESP2.
func TestGoRedisDefaultProtocolFallsBackToResp2(t *testing.T) {
	r := start(t, server.Config{})
	rdb := redis.NewClient(&redis.Options{Addr: r.addr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping with go-redis defaults: %v", err)
	}
	rdb.Set(ctx, "k", "v", 0)
	if got := check(t, rdb.Get(ctx, "k")).Val(); got != "v" {
		t.Fatalf("Get = %q", got)
	}
	if id := check(t, rdb.ClientID(ctx)).Val(); id <= 0 {
		t.Fatalf("ClientID = %d", id)
	}
}

func TestGoRedisPipeliningAndLargeValues(t *testing.T) {
	rdb := dial(t, start(t, server.Config{}).addr)

	pipe := rdb.Pipeline()
	for i := 0; i < 2000; i++ {
		pipe.Set(ctx, "p"+strconv.Itoa(i), i, 0)
	}
	if cmds, err := pipe.Exec(ctx); err != nil || len(cmds) != 2000 {
		t.Fatalf("pipeline of SETs: %d cmds, err %v", len(cmds), err)
	}
	pipe = rdb.Pipeline()
	gets := make([]*redis.StringCmd, 2000)
	for i := range gets {
		gets[i] = pipe.Get(ctx, "p"+strconv.Itoa(i))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	for i, g := range gets {
		if g.Val() != strconv.Itoa(i) {
			t.Fatalf("pipelined GET %d = %q", i, g.Val())
		}
	}

	big := strings.Repeat("0123456789abcdef", 1<<16) // 1 MiB, far beyond the parser's buffers
	if err := rdb.Set(ctx, "big", big, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got := check(t, rdb.Get(ctx, "big")).Val(); got != big {
		t.Fatalf("1MiB value corrupted (len %d)", len(got))
	}
}

func TestGoRedisConcurrentClientsDoNotLoseUpdates(t *testing.T) {
	rdb := dial(t, start(t, server.Config{}).addr)
	const goroutines, each = 50, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			own := "own:" + strconv.Itoa(g)
			for i := 0; i < each; i++ {
				if err := rdb.Incr(ctx, "hits").Err(); err != nil {
					t.Error(err)
					return
				}
				rdb.RPush(ctx, own, i)
			}
		}(g)
	}
	wg.Wait()
	if n := getInt(t, rdb, "hits"); n != goroutines*each {
		t.Fatalf("hits = %d, want %d", n, goroutines*each)
	}
	if n := check(t, rdb.LLen(ctx, "own:7")).Val(); n != each {
		t.Fatalf("LLen = %d", n)
	}
}

func TestPubSubDeliversToEverySubscriber(t *testing.T) {
	r := start(t, server.Config{})
	pub := dial(t, r.addr)

	var subs []*redis.PubSub
	for i := 0; i < 2; i++ {
		sub := dial(t, r.addr).Subscribe(ctx, "news")
		if _, err := sub.Receive(ctx); err != nil { // wait for the subscription confirmation
			t.Fatal(err)
		}
		subs = append(subs, sub)
		defer sub.Close()
	}
	if n := check(t, pub.Publish(ctx, "news", "hello")).Val(); n != 2 {
		t.Fatalf("Publish reached %d subscribers, want 2", n)
	}
	if n := check(t, pub.Publish(ctx, "other", "x")).Val(); n != 0 {
		t.Fatalf("Publish to an empty channel reached %d", n)
	}
	for i, sub := range subs {
		select {
		case m := <-sub.Channel():
			if m.Channel != "news" || m.Payload != "hello" {
				t.Fatalf("subscriber %d got %+v", i, m)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("subscriber %d never received the message", i)
		}
	}
}

// Raw sockets: exact bytes on the wire, inline commands and protocol errors.
func rawConn(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, bufio.NewReader(c)
}

func readExactly(t *testing.T, c net.Conn, br *bufio.Reader, n int) string {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, n)
	if _, err := io.ReadFull(br, buf); err != nil {
		t.Fatalf("reading %d bytes: %v (got %q)", n, err, buf)
	}
	return string(buf)
}

func TestWireFormat(t *testing.T) {
	cases := []struct{ name, send, want string }{
		{"inline ping", "PING\r\n", "+PONG\r\n"},
		{"multibulk ping", "*1\r\n$4\r\nPING\r\n", "+PONG\r\n"},
		{"pipelined inline", "PING\r\nPING\r\nPING\r\n", "+PONG\r\n+PONG\r\n+PONG\r\n"},
		{"quoted inline argument", "SET k \"a b\"\r\nGET k\r\n", "+OK\r\n$3\r\na b\r\n"},
		{"echo", "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n", "$5\r\nhello\r\n"},
		{"unknown command", "NOPE\r\n", "-ERR unknown command 'NOPE', with args beginning with: \r\n"},
		{"arity", "GET\r\n", "-ERR wrong number of arguments for 'get' command\r\n"},
		{"not an integer", "SET k v\r\nINCR k\r\n", "+OK\r\n-ERR value is not an integer or out of range\r\n"},
		{"wrongtype", "LPUSH l a\r\nGET l\r\n", ":1\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{"null bulk", "GET nothing\r\n", "$-1\r\n"},
		{"empty array", "LRANGE nothing 0 -1\r\n", "*0\r\n"},
		{"array of bulks", "RPUSH l a bb\r\nLRANGE l 0 -1\r\n", ":2\r\n*2\r\n$1\r\na\r\n$2\r\nbb\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := start(t, server.Config{})
			c, br := rawConn(t, r.addr)
			if _, err := io.WriteString(c, tc.send); err != nil {
				t.Fatal(err)
			}
			if got := readExactly(t, c, br, len(tc.want)); got != tc.want {
				t.Fatalf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

func TestProtocolErrorIsReportedAndTheConnectionClosed(t *testing.T) {
	r := start(t, server.Config{})
	c, br := rawConn(t, r.addr)
	_, _ = io.WriteString(c, "*abc\r\n")
	want := "-ERR Protocol error: invalid multibulk length\r\n"
	if got := readExactly(t, c, br, len(want)); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("the server must close the connection after a protocol error")
	}
}

func TestShutdownIsGracefulAndClosesIdleConnections(t *testing.T) {
	r := start(t, server.Config{})
	c, br := rawConn(t, r.addr)
	_, _ = io.WriteString(c, "SET k v\r\n")
	if got := readExactly(t, c, br, 5); got != "+OK\r\n" {
		t.Fatalf("got %q", got)
	}
	r.stop(t) // returns only after Serve exited and every connection goroutine finished

	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err := br.ReadByte()
	var ne net.Error
	if err == nil || (errors.As(err, &ne) && ne.Timeout()) {
		t.Fatalf("the idle connection must be closed by Shutdown, got %v", err)
	}
	if _, err := net.DialTimeout("tcp", r.addr, 500*time.Millisecond); err == nil {
		t.Fatal("the listener must be closed after Shutdown")
	}
}

func TestRestartKeepsDataAndRewriteCompactsOverTCP(t *testing.T) {
	dir := t.TempDir()
	r1 := start(t, server.Config{Dir: dir, Fsync: aof.Always})
	rdb := dial(t, r1.addr)
	for i := 0; i < 500; i++ {
		rdb.Incr(ctx, "counter")
	}
	rdb.Set(ctx, "k", "v", 0)
	rdb.RPush(ctx, "l", "a", "b")
	rdb.HSet(ctx, "h", "f", "v")
	rdb.Set(ctx, "tmp", "x", time.Hour)
	r1.stop(t)

	r2 := start(t, server.Config{Dir: dir, Fsync: aof.Always})
	rdb = dial(t, r2.addr)
	if n := getInt(t, rdb, "counter"); n != 500 {
		t.Fatalf("counter after restart = %d", n)
	}
	if got := check(t, rdb.LRange(ctx, "l", 0, -1)).Val(); strings.Join(got, "") != "ab" {
		t.Fatalf("list after restart = %v", got)
	}
	if d := check(t, rdb.TTL(ctx, "tmp")).Val(); d < 59*time.Minute {
		t.Fatalf("TTL after restart = %v", d)
	}

	if msg := check(t, rdb.BgRewriteAOF(ctx)).Val(); !strings.Contains(msg, "rewriting started") {
		t.Fatalf("BGREWRITEAOF = %q", msg)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		info := check(t, rdb.Info(ctx, "persistence")).Val()
		if strings.Contains(info, "aof_rewrite_in_progress:0") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("rewrite did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	rdb.Incr(ctx, "counter") // a write after the rewrite
	r2.stop(t)

	r3 := start(t, server.Config{Dir: dir, Fsync: aof.Always})
	rdb = dial(t, r3.addr)
	if n := getInt(t, rdb, "counter"); n != 501 {
		t.Fatalf("counter after rewrite+restart = %d", n)
	}
	if got := check(t, rdb.HGet(ctx, "h", "f")).Val(); got != "v" {
		t.Fatalf("hash after rewrite+restart = %q", got)
	}
}

func TestInfoReportsKeyspaceAndCounters(t *testing.T) {
	rdb := dial(t, start(t, server.Config{}).addr)
	rdb.Set(ctx, "a", "1", 0)
	rdb.Set(ctx, "b", "2", time.Hour)
	info := check(t, rdb.Info(ctx)).Val()
	for _, want := range []string{"# Server", "carpincho_version:", "connected_clients:", "total_commands_processed:", "db0:keys=2,expires=1"} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO is missing %q:\n%s", want, info)
		}
	}
}
