package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/MaxDCoc/carpincho/internal/resp"
)

func benchKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte("key:" + strconv.Itoa(i))
	}
	return keys
}

// BenchmarkExecute measures the command layer alone (lookup, arity check,
// handler, store, reply encoding) with no sockets, from every CPU at once.
func BenchmarkExecute(b *testing.B) {
	srv, err := New(Config{})
	if err != nil {
		b.Fatal(err)
	}
	keys := benchKeys(100_000)
	val := []byte("value-of-64-bytes-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	for _, k := range keys {
		c := newClient(srv, nil)
		c.w = resp.NewWriter(io.Discard)
		srv.execute(c, [][]byte{[]byte("SET"), k, val})
	}
	for _, name := range []string{"GET", "SET", "INCR"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				c := newClient(srv, nil)
				c.w = resp.NewWriter(io.Discard)
				i := 0
				for pb.Next() {
					k := keys[i%len(keys)]
					i++
					var args [][]byte
					switch name {
					case "GET":
						args = [][]byte{[]byte("GET"), k}
					case "SET":
						args = [][]byte{[]byte("SET"), k, val}
					case "INCR":
						args = [][]byte{[]byte("INCR"), append([]byte("ctr:"), k...)}
					}
					c.wmu.Lock()
					srv.execute(c, args)
					c.wmu.Unlock()
				}
			})
		})
	}
}

// BenchmarkTCP measures the whole path over loopback sockets: one connection per
// CPU, each sending `pipeline` commands and then reading all the replies.
func BenchmarkTCP(b *testing.B) {
	srv, err := New(Config{})
	if err != nil {
		b.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	val := "value-of-64-bytes-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	for _, name := range []string{"GET", "SET"} {
		for _, pipeline := range []int{1, 16, 128} {
			b.Run(fmt.Sprintf("%s/pipeline=%d", name, pipeline), func(b *testing.B) {
				b.RunParallel(func(pb *testing.PB) {
					conn, err := net.Dial("tcp", ln.Addr().String())
					if err != nil {
						b.Error(err)
						return
					}
					defer conn.Close()
					rd := resp.NewReader(conn)
					var batch []byte
					ends := make([]int, 0, pipeline) // ends[i] = length of a batch holding i+1 commands
					for i := 0; i < pipeline; i++ {
						key := "key:" + strconv.Itoa(i)
						if name == "GET" {
							batch = resp.AppendCommandStrings(batch, "GET", key)
						} else {
							batch = resp.AppendCommandStrings(batch, "SET", key, val)
						}
						ends = append(ends, len(batch))
					}
					for {
						// pb.Next() hands out one command at a time: collect up to
						// `pipeline` of them, send them together, read the replies.
						n := 0
						for n < pipeline && pb.Next() {
							n++
						}
						if n == 0 {
							return
						}
						if _, err := conn.Write(batch[:ends[n-1]]); err != nil {
							b.Error(err)
							return
						}
						for i := 0; i < n; i++ {
							if _, err := rd.ReadValue(); err != nil {
								b.Error(err)
								return
							}
						}
					}
				})
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "cmds/s")
			})
		}
	}
}
