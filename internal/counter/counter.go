package counter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type counterOptions struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func NewCounterOptiosn(node *maelstrom.Node) *counterOptions {
	return &counterOptions{
		node: node,
		kv:   maelstrom.NewSeqKV(node),
	}
}

func Handle(node *maelstrom.Node) {
	opt := NewCounterOptiosn(node)
	node.Handle("add", opt.add)
	node.Handle("read", opt.read)
	node.Handle("get_counter", opt.getCounter)
}

func (c *counterOptions) add(msg maelstrom.Message) error {
	body := input{}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("Send: Error while decoding json: %s", err)
	}

	oldVal, err := c.kv.ReadInt(context.TODO(), c.node.ID())
	if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		oldVal = 0
	}

	newVal := oldVal + int(body.Delta)
	c.kv.Write(context.TODO(), c.node.ID(), newVal)

	return c.node.Reply(msg, simpleOutput{Type: "add_ok"})
}

func (c *counterOptions) read(msg maelstrom.Message) error {
	sum, err := c.kv.ReadInt(context.TODO(), c.node.ID())
	if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		sum = 0
	}

	for _, id := range c.node.NodeIDs() {
		if id == c.node.ID() {
			continue
		}

		ctx , cancel := context.WithTimeout(context.TODO(), time.Millisecond * 100 )
		msg, err := c.node.SyncRPC(ctx, id, simpleOutput{Type: "get_counter"})
		cancel()
		if err != nil {
			fmt.Printf("node %s not working\n", id)
			continue
		}

		body := output{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("Add: Error while decoding json: %s", err)
		}

		sum += int(body.Value)
	}
	return c.node.Reply(msg, output{Type: "read_ok", Value: sum})
}

func (c *counterOptions) getCounter(msg maelstrom.Message) error {
	val, err := c.kv.ReadInt(context.TODO(), c.node.ID())
	if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		val = 0
	}

	return c.node.Reply(msg, output{Type: "get_counter_ok", Value: val})
}
