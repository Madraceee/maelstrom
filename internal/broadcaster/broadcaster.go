package broadcaster

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Broadcaster interface {
	Send(dst string, values ...int)
	SendTxn(dst string, values ...any)
}

type broadcasterOptions struct {
	mu         *sync.RWMutex
	dstConnMap map[string][]any
	isWorking  map[string]bool
	node       *maelstrom.Node
}

func NewBroadcaster(node *maelstrom.Node) Broadcaster {
	return &broadcasterOptions{
		mu:         &sync.RWMutex{},
		dstConnMap: make(map[string][]any),
		isWorking:  make(map[string]bool),
		node:       node,
	}
}

func (b *broadcasterOptions) Send(dst string, values ...int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.dstConnMap[dst]; !ok {
		b.dstConnMap[dst] = make([]any, 0)
	}

	anyVals := make([]any, len(values))
	for i, val := range values {
		anyVals[i] = val
	}
	b.dstConnMap[dst] = append(b.dstConnMap[dst], anyVals...)
	isWorking := b.isWorking[dst]

	if !isWorking {
		b.isWorking[dst] = true
		go func() {
			count := 0
			waitTime := 100
			contextTime := 500
			for {
				time.Sleep(time.Millisecond * time.Duration(waitTime*pow(2, count)))
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
				b.dstConnMap[dst] = b.dstConnMap[dst][len(values):]
				b.isWorking[dst] = false
				b.mu.Unlock()
				return
			}
		}()
	}
}

func (b *broadcasterOptions) SendTxn(dst string, values ...any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.dstConnMap[dst]; !ok {
		b.dstConnMap[dst] = make([]any, 0)
	}

	b.dstConnMap[dst] = append(b.dstConnMap[dst], values...)
	isWorking := b.isWorking[dst]

	if !isWorking {
		b.isWorking[dst] = true
		go func() {
			count := 0
			waitTime := 500
			for {
				contextTime := 1000
				b.mu.RLock()
				values := b.dstConnMap[dst]
				b.mu.RUnlock()
				ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*time.Duration(contextTime))
				_, err := b.node.SyncRPC(ctx, dst, map[string]interface{}{"type": "txn-write", "txn": values})
				cancel()

				if maelstrom.ErrorCode(err) == maelstrom.Timeout || errors.Is(err, context.DeadlineExceeded) {
					count++
					continue
				}

				b.mu.Lock()
				b.dstConnMap[dst] = b.dstConnMap[dst][len(values):]
				remaining := len(b.dstConnMap[dst])
				if remaining == 0 {
					b.isWorking[dst] = false
				}
				b.mu.Unlock()
				if remaining > 0 {
					time.Sleep(time.Millisecond * time.Duration(waitTime*pow(2, count)))
					continue
				}
				return
			}
		}()
	}
}

func pow(x, y int) int {
	return int(math.Pow(float64(x), float64(y)))
}
