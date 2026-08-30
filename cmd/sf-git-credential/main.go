package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nysa-company/sf/internal/gitcredential"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := gitcredential.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.LookupEnv, gitcredential.OSRunner{}); err != nil {
		// Keep the diagnostic constant: credential input and gh output are never
		// safe operator-facing error material.
		fmt.Fprintln(os.Stderr, "sf-git-credential: request refused")
		os.Exit(2)
	}
}
