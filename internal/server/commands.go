package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/MaxDCoc/carpincho/internal/store"
)

// Error replies shared by many commands. The texts are the ones Redis sends,
// so clients that match on them keep working.
const (
	errSyntax    = "ERR syntax error"
	errNotInt    = "ERR value is not an integer or out of range"
	errWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
	errOverflow  = "ERR increment or decrement would overflow"
)

// handler executes one command. It writes its reply to c.w and, if it changed
// the dataset, calls c.propagate while it still holds the shard locks.
type handler func(c *Client, args [][]byte)

type command struct {
	name       string
	arity      int // N > 0: exactly N arguments counting the name; -N: at least N
	handler    handler
	pubsubSafe bool // may run while the client is in subscribed mode
}

// registry maps lowercase command names to their implementation. It is built
// once per Server and read-only afterwards, so it needs no lock.
type registry map[string]*command

func (r registry) add(name string, arity int, h handler) *command {
	cmd := &command{name: name, arity: arity, handler: h}
	r[name] = cmd
	return cmd
}

func newRegistry() registry {
	r := registry{}
	registerConn(r)
	registerKeys(r)
	registerString(r)
	registerList(r)
	registerHash(r)
	registerSet(r)
	registerPubSub(r)
	return r
}

// lookup finds a command by name, case-insensitively, without allocating.
func (s *Server) lookup(name []byte) *command {
	var buf [32]byte
	if len(name) > len(buf) {
		return nil
	}
	for i, ch := range name {
		if 'A' <= ch && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		buf[i] = ch
	}
	return s.commands[string(buf[:len(name)])] // the compiler does not allocate for map[string(bytes)]
}

// execute validates and runs one command. The caller holds c.wmu.
func (s *Server) execute(c *Client, args [][]byte) {
	cmd := s.lookup(args[0])
	if cmd == nil {
		c.w.Error(unknownCommandMsg(args))
		return
	}
	if (cmd.arity > 0 && len(args) != cmd.arity) || len(args) < -cmd.arity {
		c.w.Error(wrongArity(cmd.name))
		return
	}
	if c.subscribed() && !cmd.pubsubSafe {
		c.w.Error("ERR Can't execute '" + cmd.name + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
		return
	}
	s.stats.commands.Add(1)
	cmd.handler(c, args)
}

func wrongArity(name string) string {
	return "ERR wrong number of arguments for '" + name + "' command"
}

func unknownCommandMsg(args [][]byte) string {
	var sb strings.Builder
	sb.WriteString("ERR unknown command '")
	sb.Write(clip(args[0]))
	sb.WriteString("', with args beginning with: ")
	for _, a := range args[1:] {
		if sb.Len() > 256 {
			break
		}
		sb.WriteByte('\'')
		sb.Write(clip(a))
		sb.WriteString("' ")
	}
	return sb.String()
}

func clip(b []byte) []byte {
	if len(b) > 128 {
		return b[:128]
	}
	return b
}

// parseInt parses a signed 64-bit integer the way Redis' string2ll does:
// canonical decimal only (no '+', no leading zeros, no spaces, no "-0").
func parseInt(b []byte) (int64, bool) {
	n := len(b)
	if n == 0 || n > 20 {
		return 0, false
	}
	if n == 1 && b[0] == '0' {
		return 0, true
	}
	i, neg := 0, false
	if b[0] == '-' {
		neg = true
		i++
		if n == 1 {
			return 0, false
		}
	}
	if b[i] < '1' || b[i] > '9' {
		return 0, false
	}
	var v uint64
	for ; i < n; i++ {
		c := b[i]
		if c < '0' || c > '9' || v > (math.MaxUint64-9)/10 {
			return 0, false
		}
		v = v*10 + uint64(c-'0')
	}
	if neg {
		if v > 1<<63 {
			return 0, false
		}
		return -int64(v), true
	}
	if v > math.MaxInt64 {
		return 0, false
	}
	return int64(v), true
}

func itoa(n int64) []byte { return strconv.AppendInt(nil, n, 10) }

// eqFold reports whether b equals the ASCII upper-case word s, ignoring case.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		c := b[i]
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

// lookupKind returns the entry under key if it has the wanted kind; wrong is
// true when the key exists but holds another kind (a WRONGTYPE error).
func lookupKind(a *store.Access, key string, kind store.Kind) (e *store.Entry, wrong bool) {
	e = a.Get(key)
	if e != nil && e.Kind != kind {
		return nil, true
	}
	return e, false
}
