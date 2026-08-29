package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/Sephy314/Cachey/internal/harness"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "run" {
		fmt.Fprintln(os.Stderr, "Usage: harness run [--verbose]")
		os.Exit(2)
	}
	verbose := len(os.Args) == 3 && os.Args[2] == "--verbose"
	if len(os.Args) > 3 || (len(os.Args) == 3 && !verbose) {
		fmt.Fprintln(os.Stderr, "Usage: harness run [--verbose]")
		os.Exit(2)
	}
	root, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "determine working directory:", err)
		os.Exit(1)
	}
	config, err := harness.LoadConfig(filepath.Join(root, ".harness", "config.toml"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	files, err := harness.LoadFiles(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result := harness.NewLoop(root, config, files, os.Stdout, verbose).Run(ctx)
	harness.PrintSummary(os.Stdout, result)
	if !result.Success {
		os.Exit(1)
	}
}
