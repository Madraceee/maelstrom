package main

import (
	"log"
	_ "maelstrom/internal/broadcast"
	"maelstrom/internal/counter"
	"maelstrom/internal/echo"
	"maelstrom/internal/generate"
	"maelstrom/internal/kafka"
	"maelstrom/internal/retry"
	"maelstrom/internal/transaction"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()


	echo.Handle(node)
	generate.Handle(node)
	//NOTE: Either run broadcast or counter due to handler conflict
	// broadcastRetryHandler := retry.NewRetryHandler(node)
	// broadcast.Handle(node, broadcastRetryHandler)

	counter.Handle(node)

	txnRetryHandler := retry.NewRetryHandler(node)
	transaction.Handle(node, txnRetryHandler)

	kafka.Handle(node)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
