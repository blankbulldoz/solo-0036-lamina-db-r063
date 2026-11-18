package main

import (
	"context"
	"flag"
	"fmt"
	"lamina/internal/engine"
	"lamina/internal/snapshot"
	"lamina/internal/transport"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("lamina stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	dir := flag.String("data", "var/lamina", "data directory exclusively owned by this process")
	addr := flag.String("listen", "127.0.0.1:7386", "HTTP bind address")
	restore := flag.String("restore", "", "validated backup to install into a new data directory before serving")
	drain := flag.Duration("drain-timeout", 10*time.Second, "bounded wait for in-flight requests after SIGTERM/SIGINT before forcing close; acknowledged commits never depend on this budget")
	healthcheck := flag.Bool("healthcheck", false, "probe the health endpoint of the instance at -listen and exit")
	flag.Parse()
	if *healthcheck {
		return probe(*addr)
	}
	if *restore != "" {
		info, err := os.Stat(*restore)
		if err != nil {
			return err
		}
		if info.Size() > snapshot.MaxImage {
			return fmt.Errorf("backup exceeds limit")
		}
		b, err := os.ReadFile(*restore)
		if err != nil {
			return err
		}
		if err = engine.Restore(*dir, b); err != nil {
			return err
		}
	}
	db, err := engine.Open(*dir)
	if err != nil {
		return err
	}
	defer func() {
		started := time.Now()
		if err := db.Close(); err != nil {
			slog.Error("database close failed", "error", err)
			return
		}
		slog.Info("database closed", "close_elapsed", time.Since(started).Round(time.Millisecond))
	}()
	server := &http.Server{Addr: *addr, Handler: transport.New(db), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 45 * time.Second, MaxHeaderBytes: 32 << 10}
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stopped)
	finished := make(chan error, 1)
	go func() {
		slog.Info("lamina ready", "address", *addr, "data", *dir, "drain_timeout", drain.String())
		finished <- server.ListenAndServe()
	}()
	select {
	case err = <-finished:
		if err != http.ErrServerClosed {
			return err
		}
	case <-stopped:
		started := time.Now()
		slog.Info("shutdown requested; draining in-flight requests", "budget", drain.String())
		ctx, cancel := context.WithTimeout(context.Background(), *drain)
		err = server.Shutdown(ctx)
		cancel()
		if err != nil {
			// The drain budget is exhausted, not durability: every acknowledged
			// commit was already synced to the journal before it was published.
			// Forcing the close only abandons unacknowledged requests, whose
			// outcome callers must reconcile after restart.
			slog.Warn("drain budget exhausted; forcing close; acknowledged commits remain durable", "error", err)
			server.Close()
		}
		slog.Info("http drained", "elapsed", time.Since(started).Round(time.Millisecond))
	}
	return nil
}

// probe lets the minimal container image health-check itself without curl or a
// shell. Wildcard bind addresses are probed through the loopback interface.
func probe(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("healthcheck address: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/v1/lamina/health")
	if err != nil {
		return fmt.Errorf("health probe: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health probe: status %d", resp.StatusCode)
	}
	return nil
}
