package main

import (
	"context"
	"errors"
	"log"
	"math"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcaster struct {
	mu         *sync.RWMutex
	dstConnMap map[string]bool
	node       *maelstrom.Node
	store      *store
}

func NewBroadcaster(node *maelstrom.Node, store *store) *broadcaster {
	return &broadcaster{
		mu:         &sync.RWMutex{},
		dstConnMap: make(map[string]bool),
		node:       node,
		store:      store,
	}
}

func (b *broadcaster) Send(value int, dst string) {
	go func() {
		nodeId := b.node.ID()
		backoff := 100
		contextTime := 500
		log.Printf("Sending from %s to %s value %d", nodeId, dst, value)
		ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*time.Duration(contextTime))
		_, err := b.node.SyncRPC(ctx, dst, map[string]interface{}{"type": "broadcast", "message": value})
		cancel()

		count := 1
		for maelstrom.ErrorCode(err) == maelstrom.Timeout || errors.Is(err, context.DeadlineExceeded) {
			waitTime := backoff * pow(2, count)
			time.Sleep(time.Millisecond * time.Duration(waitTime))
			log.Printf("Trying to send from %s to %s value %d", nodeId, dst, value)

			ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*time.Duration(contextTime))
			_, err = b.node.SyncRPC(ctx, dst, map[string]interface{}{"type": "broadcast", "message": value})
			cancel()
			count++
		}
		log.Printf("Sent from %s to %s of value %d with count %d", nodeId, dst, value, count)
	}()
}

func pow(x, y int) int {
	return int(math.Pow(float64(x), float64(y)))
}
