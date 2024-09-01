package main

import (
	"log/slog"

	server "github.com/alexPavlikov/gora_geo-search_service/cmd"
)

func main() {

	if err := server.Run(); err != nil {
		slog.Error("run error", "error", err)
		return
	}
}
