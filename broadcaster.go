package main

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcaster struct {
	mu         *sync.RWMutex
	dstConnMap map[string][]int
	node       *maelstrom.Node
	store      *store
}

func NewBroadcaster(node *maelstrom.Node, store *store) *broadcaster {
	return &broadcaster{
		mu:         &sync.RWMutex{},
		dstConnMap: make(map[string][]int),
		node:       node,
		store:      store,
	}
}

func (b *broadcaster) Send(dst string, values ...int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.dstConnMap[dst]

	if !ok {
		b.dstConnMap[dst] = make([]int, 0)
		go func() {
			count := 0
			for {
				waitTime := 100
				time.Sleep(time.Millisecond * time.Duration(waitTime*pow(2, count)))

				contextTime := 1000
				b.mu.RLock()
				values := b.dstConnMap[dst]
				b.mu.RUnlock()
				ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*time.Duration(contextTime))
				_, err := b.node.SyncRPC(ctx, dst, map[string]interface{}{"type": "broadcast-group", "message": values})
				cancel()

				if maelstrom.ErrorCode(err) == maelstrom.Timeout || errors.Is(err, context.DeadlineExceeded) {
					count++
					continue
				}

				b.mu.Lock()
				delete(b.dstConnMap, dst)
				b.mu.Unlock()
				return
			}
		}()
	}

	b.dstConnMap[dst] = append(b.dstConnMap[dst], values...)
}

func pow(x, y int) int {
	return int(math.Pow(float64(x), float64(y)))
}
