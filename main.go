package main

import (
	"log"
	"maelstrom/internal/broadcast"
	"maelstrom/internal/broadcaster"
	_ "maelstrom/internal/counter"
	"maelstrom/internal/echo"
	"maelstrom/internal/generate"
	"maelstrom/internal/kafka"
	"maelstrom/internal/transaction"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	broadcaster := broadcaster.NewBroadcaster(node)

	echo.Handle(node)
	generate.Handle(node)
	broadcast.Handle(node, broadcaster)
	// counter.Handle(node)
	transaction.Handle(node, broadcaster)
	kafka.Handle(node)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
