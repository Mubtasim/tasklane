package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

// Config is the server configuration, read from TASKLANE_* environment
// variables.
type Config struct {
	DatabaseURL    string        // TASKLANE_DATABASE_URL, required
	GRPCAddr       string        // TASKLANE_GRPC_ADDR, default :7070
	HTTPAddr       string        // TASKLANE_HTTP_ADDR, default :8080
	APIKeys        []string      // TASKLANE_API_KEYS, comma-separated; empty disables auth
	ReaperInterval time.Duration // TASKLANE_REAPER_INTERVAL, default 5s
	Retention      time.Duration // TASKLANE_RETENTION, default 24h: completed and cancelled jobs
	DeadRetention  time.Duration // TASKLANE_DEAD_RETENTION, default 168h: dead jobs
	PruneInterval  time.Duration // TASKLANE_PRUNE_INTERVAL, default 1m
}

// LoadConfig reads the configuration through getenv (os.Getenv in main), so
// tests can pass a map lookup instead of touching the process environment.
func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL: strings.TrimSpace(getenv("TASKLANE_DATABASE_URL")),
		GRPCAddr:    or(getenv("TASKLANE_GRPC_ADDR"), ":7070"),
		HTTPAddr:    or(getenv("TASKLANE_HTTP_ADDR"), ":8080"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("TASKLANE_DATABASE_URL is required")
	}
	for _, k := range strings.Split(getenv("TASKLANE_API_KEYS"), ",") {
		if k = strings.TrimSpace(k); k != "" {
			cfg.APIKeys = append(cfg.APIKeys, k)
		}
	}
	durations := []struct {
		key      string
		fallback time.Duration
		dst      *time.Duration
	}{
		{"TASKLANE_REAPER_INTERVAL", 5 * time.Second, &cfg.ReaperInterval},
		{"TASKLANE_RETENTION", 24 * time.Hour, &cfg.Retention},
		{"TASKLANE_DEAD_RETENTION", 168 * time.Hour, &cfg.DeadRetention},
		{"TASKLANE_PRUNE_INTERVAL", time.Minute, &cfg.PruneInterval},
	}
	for _, d := range durations {
		v, err := positiveDuration(getenv, d.key, d.fallback)
		if err != nil {
			return Config{}, err
		}
		*d.dst = v
	}
	return cfg, nil
}

// positiveDuration reads key as a duration such as 5s or 24h; unset means
// fallback.
func positiveDuration(getenv func(string) string, key string, fallback time.Duration) (time.Duration, error) {
	v := getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration such as 5s, got %q", key, v)
	}
	return d, nil
}

func or(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// dialAddr turns a listen address into one the in-process gateway can dial:
// ":7070" and "0.0.0.0:7070" listen on every interface, so dial loopback.
func dialAddr(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return net.JoinHostPort(host, port), nil
}
