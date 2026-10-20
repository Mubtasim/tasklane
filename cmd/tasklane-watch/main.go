// Command tasklane-watch shows a queue's live counts in the terminal,
// redrawn every second: available, leased and dead jobs, and how many jobs
// complete per second.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

func main() {
	addr := flag.String("addr", envOr("TASKLANE_ADDR", "localhost:7070"), "server gRPC address")
	// The key comes from the environment after parsing, not as the flag
	// default, so -h doesn't print it.
	apiKey := flag.String("api-key", "", "API key, if the server requires one (default $TASKLANE_API_KEY)")
	queue := flag.String("queue", "demo", "queue to watch")
	interval := flag.Duration("interval", time.Second, "refresh interval")
	flag.Parse()
	if *apiKey == "" {
		*apiKey = os.Getenv("TASKLANE_API_KEY")
	}

	var opts []tasklane.Option
	if *apiKey != "" {
		opts = append(opts, tasklane.WithAPIKey(*apiKey))
	}
	client, err := tasklane.Dial(*addr, opts...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tasklane-watch:", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	tick := time.NewTicker(*interval)
	defer tick.Stop()
	for {
		reqCtx, cancel := context.WithTimeout(ctx, *interval)
		info, err := client.GetQueue(reqCtx, *queue)
		cancel()
		if ctx.Err() != nil {
			fmt.Println()
			return
		}
		// \r returns to the line start and \033[2K clears it, so the line
		// is redrawn in place.
		fmt.Print("\r\033[2K" + frame(*queue, info.Stats, err))
		select {
		case <-ctx.Done():
			fmt.Println()
			return
		case <-tick.C:
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
