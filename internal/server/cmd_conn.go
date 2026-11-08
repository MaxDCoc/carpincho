package server

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MaxDCoc/carpincho/internal/store"
)

func registerConn(r registry) {
	r.add("ping", -1, cmdPing).pubsubSafe = true
	r.add("echo", 2, func(c *Client, args [][]byte) { c.w.Bulk(args[1]) })
	r.add("quit", -1, func(c *Client, args [][]byte) { c.w.SimpleString("OK"); c.quit = true }).pubsubSafe = true
	r.add("hello", -1, cmdHello)
	r.add("command", -1, cmdCommand)
	r.add("info", -1, cmdInfo)
	r.add("client", -2, cmdClient)
	r.add("config", -2, cmdConfig)
	r.add("select", 2, cmdSelect)
	r.add("bgrewriteaof", 1, cmdBgRewriteAOF)
}

func cmdPing(c *Client, args [][]byte) {
	if len(args) > 2 {
		c.w.Error(wrongArity("ping"))
		return
	}
	if c.subscribed() { // in subscribed mode PING answers with a pubsub-style array
		c.w.ArrayHeader(2)
		c.w.BulkString("pong")
		if len(args) == 2 {
			c.w.Bulk(args[1])
		} else {
			c.w.BulkString("")
		}
		return
	}
	if len(args) == 2 {
		c.w.Bulk(args[1])
		return
	}
	c.w.SimpleString("PONG")
}

// validClientName accepts printable ASCII without spaces, like Redis.
func validClientName(b []byte) bool {
	for _, ch := range b {
		if ch < '!' || ch > '~' {
			return false
		}
	}
	return true
}

// cmdHello implements HELLO for protocol 2 only. HELLO 3 is answered with
// -NOPROTO, which well-behaved clients (go-redis, redis-py, ...) take as
// "stay on RESP2".
func cmdHello(c *Client, args [][]byte) {
	i := 1
	if len(args) > 1 {
		v, ok := parseInt(args[1])
		if !ok {
			c.w.Error("ERR Protocol version is not an integer or out of range")
			return
		}
		switch {
		case v < 2 || v > 3:
			c.w.Error("NOPROTO unsupported protocol version")
			return
		case v == 3:
			c.w.Error("NOPROTO sorry, this protocol version is not supported")
			return
		}
		i = 2
	}
	for i < len(args) {
		switch {
		case eqFold(args[i], "AUTH") && i+2 < len(args):
			i += 3 // no password is configured, so any credentials are accepted
		case eqFold(args[i], "SETNAME") && i+1 < len(args):
			if !validClientName(args[i+1]) {
				c.w.Error("ERR Client names cannot contain spaces, newlines or special characters.")
				return
			}
			c.name = string(args[i+1])
			i += 2
		default:
			c.w.Error("ERR Syntax error in HELLO option '" + string(args[i]) + "'")
			return
		}
	}
	c.w.ArrayHeader(14)
	for _, kv := range [][2]string{{"server", "redis"}, {"version", "7.0.0"}} {
		c.w.BulkString(kv[0])
		c.w.BulkString(kv[1])
	}
	c.w.BulkString("proto")
	c.w.Int(2)
	c.w.BulkString("id")
	c.w.Int(c.id)
	for _, kv := range [][2]string{{"mode", "standalone"}, {"role", "master"}} {
		c.w.BulkString(kv[0])
		c.w.BulkString(kv[1])
	}
	c.w.BulkString("modules")
	c.w.ArrayHeader(0)
}

// cmdCommand is a stub so that redis-cli and client libraries that call COMMAND
// (or COMMAND DOCS) at startup get a valid, empty answer.
func cmdCommand(c *Client, args [][]byte) {
	if len(args) >= 2 {
		switch {
		case eqFold(args[1], "COUNT"):
			c.w.Int(int64(len(c.srv.commands)))
			return
		case eqFold(args[1], "LIST"):
			names := make([]string, 0, len(c.srv.commands))
			for n := range c.srv.commands {
				names = append(names, n)
			}
			sort.Strings(names)
			c.w.ArrayHeader(len(names))
			for _, n := range names {
				c.w.BulkString(n)
			}
			return
		}
	}
	c.w.ArrayHeader(0)
}

func cmdClient(c *Client, args [][]byte) {
	switch {
	case eqFold(args[1], "SETNAME"):
		if len(args) != 3 {
			c.w.Error(wrongArity("client|setname"))
			return
		}
		if !validClientName(args[2]) {
			c.w.Error("ERR Client names cannot contain spaces, newlines or special characters.")
			return
		}
		c.name = string(args[2])
		c.w.SimpleString("OK")
	case eqFold(args[1], "GETNAME"):
		if c.name == "" {
			c.w.Null()
		} else {
			c.w.BulkString(c.name)
		}
	case eqFold(args[1], "ID"):
		c.w.Int(c.id)
	case eqFold(args[1], "SETINFO"): // sent by go-redis and others right after connecting
		if len(args) != 4 {
			c.w.Error(wrongArity("client|setinfo"))
			return
		}
		c.w.SimpleString("OK")
	default:
		c.w.Error("ERR unknown subcommand '" + string(args[1]) + "'. Try CLIENT HELP.")
	}
}

func cmdSelect(c *Client, args [][]byte) {
	n, ok := parseInt(args[1])
	switch {
	case !ok:
		c.w.Error(errNotInt)
	case n != 0:
		c.w.Error("ERR DB index is out of range")
	default:
		c.w.SimpleString("OK")
	}
}

func (s *Server) configParams() map[string]string {
	yn := "no"
	fsync := "everysec"
	if s.aof != nil {
		yn, fsync = "yes", s.aof.Policy().String()
	}
	return map[string]string{
		"appendonly":  yn,
		"appendfsync": fsync,
		"save":        "",
		"dir":         s.cfg.Dir,
		"databases":   "1",
		"maxmemory":   "0",
		"hz":          "10",
	}
}

// cmdConfig supports CONFIG GET, which redis-benchmark calls to print the host
// configuration. Everything is read-only.
func cmdConfig(c *Client, args [][]byte) {
	if !eqFold(args[1], "GET") {
		c.w.Error("ERR unknown subcommand '" + string(args[1]) + "'. Try CONFIG HELP.")
		return
	}
	if len(args) != 3 {
		c.w.Error(wrongArity("config|get"))
		return
	}
	params := c.srv.configParams()
	pattern := strings.ToLower(string(args[2]))
	var names []string
	for k := range params {
		if store.GlobMatch(pattern, k) {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	c.w.ArrayHeader(2 * len(names))
	for _, n := range names {
		c.w.BulkString(n)
		c.w.BulkString(params[n])
	}
}

func cmdBgRewriteAOF(c *Client, _ [][]byte) {
	if c.srv.aof == nil {
		c.w.Error("ERR AOF is not enabled: start carpincho with --dir to persist data")
		return
	}
	if err := c.srv.startRewrite(); err != nil {
		c.w.Error("ERR " + err.Error())
		return
	}
	c.w.SimpleString("Background append only file rewriting started")
}

func cmdInfo(c *Client, args [][]byte) {
	section := "all"
	if len(args) > 1 {
		section = strings.ToLower(string(args[1]))
	}
	c.w.BulkString(c.srv.info(section))
}

func (s *Server) info(section string) string {
	s.mu.Lock()
	clients := len(s.conns)
	port := ""
	if s.ln != nil {
		if _, p, err := net.SplitHostPort(s.ln.Addr().String()); err == nil {
			port = p
		}
	}
	s.mu.Unlock()

	aofEnabled := 0
	if s.aof != nil {
		aofEnabled = 1
	}
	rewriting := 0
	if s.rewriting.Load() {
		rewriting = 1
	}
	var sb strings.Builder
	want := func(name string) bool { return section == "all" || section == "everything" || section == name }
	if want("server") {
		fmt.Fprintf(&sb, "# Server\r\ncarpincho_version:%s\r\nredis_version:7.0.0\r\nredis_mode:standalone\r\nos:%s\r\narch_bits:64\r\ngo_version:%s\r\nprocess_id:%d\r\ntcp_port:%s\r\nuptime_in_seconds:%d\r\n\r\n",
			Version, runtime.GOOS, runtime.Version(), os.Getpid(), port, int64(time.Since(s.startedAt).Seconds()))
	}
	if want("clients") {
		fmt.Fprintf(&sb, "# Clients\r\nconnected_clients:%d\r\n\r\n", clients)
	}
	if want("stats") {
		fmt.Fprintf(&sb, "# Stats\r\ntotal_connections_received:%d\r\ntotal_commands_processed:%d\r\n\r\n", s.stats.connections.Load(), s.stats.commands.Load())
	}
	if want("persistence") {
		fmt.Fprintf(&sb, "# Persistence\r\naof_enabled:%d\r\naof_rewrite_in_progress:%d\r\n", aofEnabled, rewriting)
		if s.aof != nil {
			fmt.Fprintf(&sb, "appendfsync:%s\r\naof_current_size:%s\r\n", s.aof.Policy(), strconv.FormatInt(s.aof.Size(), 10))
		}
		sb.WriteString("\r\n")
	}
	if want("keyspace") {
		sb.WriteString("# Keyspace\r\n")
		if n := s.store.Len(); n > 0 {
			fmt.Fprintf(&sb, "db0:keys=%d,expires=%d,avg_ttl=0\r\n", n, s.store.ExpiresLen())
		}
		sb.WriteString("\r\n")
	}
	return sb.String()
}
