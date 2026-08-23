package echo

import (
	"encoding/json"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type config struct {
	node *maelstrom.Node
}

func newConfig(node *maelstrom.Node) *config {
	return &config{
		node: node,
	}
}

func Handle(node *maelstrom.Node) {
	cfg := newConfig(node)
	node.Handle("echo", cfg.echo)
}

func (c *config) echo(msg maelstrom.Message) error {
	input := body{}
	if err := json.Unmarshal(msg.Body, &input); err != nil {
		return fmt.Errorf("ECHO: Error while decoding json: %s", err)
	}

	return c.node.Reply(msg, map[string]any{"type": "echo_ok", "echo": input.Echo})
}
