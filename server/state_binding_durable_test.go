//go:build linux

package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sozercan/vekil/auth"
	"github.com/sozercan/vekil/logger"
	"github.com/sozercan/vekil/proxy"
)

func TestDurableStateConstructorFailureReleasesLock(t *testing.T) {
	t.Setenv("LIGHTWEIGHT_API_KEY", "synthetic-lightweight-key")
	t.Setenv("POWERFUL_API_KEY", "synthetic-powerful-key")
	cfg, err := proxy.LoadProvidersConfigFile("../examples/policy-routing-coding-economy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	opts := WithProxyOptions(proxy.WithProvidersConfig(cfg), proxy.WithDurableStateBindings(proxy.DurableStateBindingsConfig{Path: filepath.Join(dir, "state.db")}))
	_, err = New(auth.NewTestAuthenticator("synthetic-token"), logger.New(logger.LevelFatal), "0.0.0.0", "0", opts)
	if err == nil || !strings.Contains(err.Error(), "non-loopback") {
		t.Fatalf("constructor = %v, want late listen-host rejection", err)
	}
	// The second constructor must acquire the same store, without serving or
	// contacting the example providers. A leaked descriptor would hold its lock.
	srv, err := New(auth.NewTestAuthenticator("synthetic-token"), logger.New(logger.LevelFatal), "127.0.0.1", "0", opts)
	if err != nil {
		t.Fatal(err)
	}
	srv.proxyHandler.BeginShutdown()
	if err := srv.proxyHandler.WaitLifecycleWorkers(context.Background()); err != nil {
		t.Fatal(err)
	}
}
