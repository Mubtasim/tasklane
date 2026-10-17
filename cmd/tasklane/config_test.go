package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig(env(map[string]string{"TASKLANE_DATABASE_URL": "postgres://x"}))
	require.NoError(t, err)
	require.Equal(t, Config{
		DatabaseURL:    "postgres://x",
		GRPCAddr:       ":7070",
		HTTPAddr:       ":8080",
		ReaperInterval: 5 * time.Second,
	}, cfg)
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := LoadConfig(env(map[string]string{
		"TASKLANE_DATABASE_URL":    "postgres://x",
		"TASKLANE_GRPC_ADDR":       "127.0.0.1:9000",
		"TASKLANE_HTTP_ADDR":       ":9001",
		"TASKLANE_API_KEYS":        " key-one, ,key-two ",
		"TASKLANE_REAPER_INTERVAL": "250ms",
	}))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9000", cfg.GRPCAddr)
	require.Equal(t, ":9001", cfg.HTTPAddr)
	require.Equal(t, []string{"key-one", "key-two"}, cfg.APIKeys)
	require.Equal(t, 250*time.Millisecond, cfg.ReaperInterval)
}

func TestLoadConfigErrors(t *testing.T) {
	_, err := LoadConfig(env(map[string]string{}))
	require.ErrorContains(t, err, "TASKLANE_DATABASE_URL")

	for _, bad := range []string{"soon", "0s", "-5s"} {
		_, err = LoadConfig(env(map[string]string{"TASKLANE_DATABASE_URL": "postgres://x", "TASKLANE_REAPER_INTERVAL": bad}))
		require.ErrorContains(t, err, "TASKLANE_REAPER_INTERVAL", bad)
	}
}

func TestDialAddr(t *testing.T) {
	for listen, want := range map[string]string{
		":7070":         "localhost:7070",
		"0.0.0.0:7070":  "localhost:7070",
		"[::]:7070":     "localhost:7070",
		"10.0.0.5:7070": "10.0.0.5:7070",
	} {
		got, err := dialAddr(listen)
		require.NoError(t, err)
		require.Equal(t, want, got, listen)
	}
	_, err := dialAddr("7070")
	require.Error(t, err)
}
