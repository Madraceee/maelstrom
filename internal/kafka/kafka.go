package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type record struct {
	offset int
	msg    int
}

type config struct {
	node *maelstrom.Node

	linKv *maelstrom.KV
	seqKv *maelstrom.KV
}

func newConfig(node *maelstrom.Node) *config {
	return &config{
		node:  node,
		linKv: maelstrom.NewLinKV(node),
		seqKv: maelstrom.NewSeqKV(node),
	}
}

func Handle(node *maelstrom.Node) {
	config := newConfig(node)
	node.Handle("send", config.send)
	node.Handle("poll", config.poll)
	node.Handle("commit_offsets", config.commitOffsets)
	node.Handle("list_committed_offsets", config.listCommitOffsets)
}

func (c *config) send(msg maelstrom.Message) error {
	body := sendInput{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("Send: Error while decoding json: %s", err)
	}

	offset := 0
	retry := 0
	for retry < 10 {
		offset = 0
		response := make(map[int]int)
		err := c.linKv.ReadInto(context.TODO(), body.Key, &response)
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			response[offset] = body.Msg
			if err := c.linKv.CompareAndSwap(context.TODO(), body.Key, response, response, true); err != nil {
				retry++
				continue
			}
		} else {
			offset = offset + len(response)
			newReponse := maps.Clone(response)

			newReponse[offset] = body.Msg
			if err := c.linKv.CompareAndSwap(context.TODO(), body.Key, response, newReponse, false); err != nil {
				retry++
				continue
			}
		}
		break
	}
	if retry >= 10 {
		return fmt.Errorf("Send: could not set value")
	}

	return c.node.Reply(msg, sendOutput{Type: "send_ok", Offset: offset})

}

func (c *config) poll(msg maelstrom.Message) error {
	input := pollInput{}
	if err := json.Unmarshal(msg.Body, &input); err != nil {
		return fmt.Errorf("Poll: Error while decoding json: %s", err)
	}

	msgs := make(map[string][][2]int)
	for key, startOffset := range input.Offsets {
		body := make(map[int]int)
		if err := c.linKv.ReadInto(context.TODO(), key, &body); maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			continue
		}
		for offset, val := range body {
			if offset >= startOffset {
				msgs[key] = append(msgs[key], [2]int{offset, int(val)})
			}
		}
	}

	for _, val := range msgs {
		slices.SortFunc(val, func(i, j [2]int) int {
			return i[0] - j[0]
		})
	}

	return c.node.Reply(msg, pollOutput{Type: "poll_ok", Msgs: msgs})
}

func (c *config) commitOffsets(msg maelstrom.Message) error {
	body := commitOffsetsInput{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("COMMIT_OFFSETS: Error while decoding json: %s", err)
	}

	for key, offset := range body.Offsets {
		for {
			val, err := c.seqKv.ReadInt(context.TODO(), key+"Commit")
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				if err := c.seqKv.CompareAndSwap(context.TODO(), key+"Commit", val, offset, true); err != nil {
					continue
				}
			} else if err != nil {
				return fmt.Errorf("commit_offsets: Could not fetch key %s details: %s", key, err)
			} else {
				offset = max(val, offset)
				if err := c.seqKv.CompareAndSwap(context.TODO(), key+"Commit", val, offset, false); err != nil {
					continue
				}
			}
			break
		}
	}

	return c.node.Reply(msg, commitOffsetsOutput{Type: "commit_offsets_ok"})
}

func (c *config) listCommitOffsets(msg maelstrom.Message) error {
	body := listCommitOffsetsInput{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("LIST_COMMITED_OFFSETS: Error while decoding json: %s", err)
	}

	result := make(map[string]int)
	for _, key := range body.Keys {
		val, err := c.seqKv.ReadInt(context.TODO(), key+"Commit")
		if err != nil {
			continue
		}
		result[key] = val
	}

	return c.node.Reply(msg, listCommitOffsetsOutput{Type: "list_committed_offsets_ok", Offsets: result})
}
