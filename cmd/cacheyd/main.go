package main

import (
	"os"
	"time"

	"github.com/Sephy314/Cachey/internal/server"
	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/internal/wal"
)

func main() {
	if len(os.Args) < 2 {
		println("Usage: cacheyd <address> [data-dir]")
		os.Exit(1)
	}

	addr := os.Args[1]
	dir := "data"
	if len(os.Args) >= 3 {
		dir = os.Args[2]
	}

	st := store.NewCacheyStore()

	cfg := wal.DefaultConfig(dir)
	w, err := wal.Open(cfg, wal.Hooks{
		ApplySnapshot: st.ApplySnapshot,
		ApplyRecord:   st.ApplyRecord,
		Snapshot:      st.Snapshot,
	})
	if err != nil {
		println("Error opening WAL:", err.Error())
		os.Exit(1)
	}
	defer w.Close()
	st.SetWAL(w)

	st.StartActiveExpiration(1 * time.Second)
	hdl := server.NewCacheyHandler(st)
	server := server.NewServer(addr, hdl)

	err = server.Start()
	if err != nil {
		println("Error starting server:", err.Error())
		os.Exit(1)
	}

	println("Server started on", addr, "with WAL at", dir)
	select {}
}
