package main

import (
	"os"
	"time"

	"github.com/Sephy314/Cachey/internal/server"
	"github.com/Sephy314/Cachey/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		println("Usage: cacheyd <address>")
		os.Exit(1)
	}

	addr := os.Args[1]

	store := store.NewCacheyStore()
	store.StartActiveExpiration(1 * time.Second)
	hdl := server.NewCacheyHandler(store)
	server := server.NewServer(addr, hdl)

	err := server.Start()
	if err != nil {
		println("Error starting server:", err.Error())
		os.Exit(1)
	}

	println("Server started on", addr)
	select {}
}
