package counter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type config struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func newConfig(node *maelstrom.Node) *config {
	return &config{
		node: node,
		kv:   maelstrom.NewSeqKV(node),
	}
}

func Handle(node *maelstrom.Node) {
	opt := newConfig(node)
	node.Handle("add", opt.add)
	node.Handle("read", opt.read)
	node.Handle("get_counter", opt.getCounter)
}

func (c *config) add(msg maelstrom.Message) error {
	body := input{}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("Add: Error while decoding json: %s", err)
	}

	for {
		oldVal, err := c.kv.ReadInt(context.TODO(), c.node.ID())
		newVal := oldVal + int(body.Delta)
		isNewKey := maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist
		if err := c.kv.CompareAndSwap(context.TODO(), c.node.ID(), oldVal, newVal, isNewKey); err != nil {
			continue
		}
		break
	}

	return c.node.Reply(msg, simpleOutput{Type: "add_ok"})
}

func (c *config) read(msg maelstrom.Message) error {
	sum, err := c.kv.ReadInt(context.TODO(), c.node.ID())
	if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		sum = 0
	}

	for _, id := range c.node.NodeIDs() {
		if id == c.node.ID() {
			continue
		}

		ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*200)
		recvMsg, err := c.node.SyncRPC(ctx, id, simpleOutput{Type: "get_counter"})
		cancel()
		if err != nil {
			continue
		}

		body := output{}
		if err := json.Unmarshal(recvMsg.Body, &body); err != nil {
			return fmt.Errorf("Read: Error while decoding json: %s", err)
		}

		sum += int(body.Value)
	}
	return c.node.Reply(msg, output{Type: "read_ok", Value: sum})
}

func (c *config) getCounter(msg maelstrom.Message) error {
	val, err := c.kv.ReadInt(context.TODO(), c.node.ID())
	if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		val = 0
	}

	return c.node.Reply(msg, output{Type: "get_counter_ok", Value: val})
}
