package echo

import (
	"encoding/json"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func Handle(node *maelstrom.Node) {
	node.Handle("echo", func(msg maelstrom.Message) error {
		reply := echo(msg)
		return node.Reply(msg, reply)
	})
}

func echo(m maelstrom.Message) any {
	msg := body{}
	if err := json.Unmarshal(m.Body, &msg); err != nil {
		return fmt.Errorf("ECHO: Error while decoding json: %s", err)
	}

	return map[string]any{"type": "echo_ok", "echo": msg.Echo}
}
