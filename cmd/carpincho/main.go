// Command carpincho is a Redis-compatible in-memory database server.
//
//	carpincho --port 6380 --dir ./data --appendfsync everysec
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/MaxDCoc/carpincho/internal/aof"
	"github.com/MaxDCoc/carpincho/internal/server"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("carpincho: %v", err)
	}
}

func run() error {
	var (
		port    = flag.Int("port", 6380, "TCP port to listen on (6380 so it never clashes with a real Redis on 6379)")
		bind    = flag.String("bind", "127.0.0.1", "address to listen on; use 0.0.0.0 inside a container")
		dir     = flag.String("dir", "", "data directory for the append-only file; empty keeps everything in memory only")
		fsync   = flag.String("appendfsync", "everysec", "when to fsync the append-only file: always, everysec or no")
		version = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()
	if *version {
		fmt.Println("carpincho", server.Version)
		return nil
	}
	if *port < 0 || *port > 65535 {
		return fmt.Errorf("invalid --port %d", *port)
	}
	policy, err := aof.ParsePolicy(*fsync)
	if err != nil {
		return err
	}
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	if *dir == "" {
		logger.Println("no --dir given: running in memory only, data is lost on exit")
	}

	srv, err := server.New(server.Config{Dir: *dir, Fsync: policy, Logger: logger})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(*bind, strconv.Itoa(*port)))
	if err != nil {
		return err
	}
	logger.Printf("carpincho %s ready on %s", server.Version, ln.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	logger.Println("signal received: draining connections and flushing the append-only file")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-serveErr; err != nil && !errors.Is(err, server.ErrServerClosed) {
		return err
	}
	logger.Println("bye")
	return nil
}
