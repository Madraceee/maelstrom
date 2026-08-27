package main

import (
	"log"
	_ "maelstrom/internal/broadcast"
	"maelstrom/internal/retry"
	"maelstrom/internal/counter"
	"maelstrom/internal/echo"
	"maelstrom/internal/generate"
	"maelstrom/internal/kafka"
	"maelstrom/internal/transaction"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	retryHandler := retry.NewRetryHandler(node)

	echo.Handle(node)
	generate.Handle(node)
	// broadcast.Handle(node, retryHandler)
	counter.Handle(node)
	transaction.Handle(node, retryHandler)
	kafka.Handle(node)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
