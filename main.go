package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maelstrom/internal/availability"
	"maelstrom/internal/broadcast"
	"maelstrom/internal/broadcaster"
	_ "maelstrom/internal/counter"
	"maelstrom/internal/echo"
	"maelstrom/internal/generate"
	"maps"
	"slices"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	broadcaster := broadcaster.NewBroadcaster(node)

	echo.Handle(node)
	generate.Handle(node)
	broadcast.Handle(node, broadcaster)
	// counter.Handle(node)
	availability.Handle(node, broadcaster)

	// KAFKA
	type record struct {
		offset int
		msg    int
	}
	linKv := maelstrom.NewLinKV(node)
	seqKv := maelstrom.NewSeqKV(node)

	node.Handle("send", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("Send: Error while decoding json: %s", err)
		}

		key, ok := body["key"].(string)
		if !ok {
			return fmt.Errorf("Send: key does not exists")
		}

		// keyInt, _ := strconv.Atoi(key)
		// targetNode := node.NodeIDs()[keyInt%len(node.NodeIDs())]
		// if node.ID() != targetNode {
		// 	msg, err := node.SyncRPC(context.TODO(), targetNode, body)
		// 	if err != nil {
		// 		return fmt.Errorf("Send: Could not get key from targetNode %s: %s", targetNode, err)
		// 	}
		//
		// 	if err := json.Unmarshal(msg.Body, &body); err != nil {
		// 		return fmt.Errorf("Send: Error while decoding json: %s", err)
		// 	}
		// 	return node.Reply(msg, map[string]interface{}{"type": "send_ok", "offset": body["offset"]})
		// }

		value, ok := body["msg"].(float64)
		if !ok {
			return fmt.Errorf("Send: key does not exists")
		}

		for {
			offset := 1000
			body := make(map[int]float64)
			err := linKv.ReadInto(context.TODO(), key, &body)
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				body[offset] = value
				if err := linKv.CompareAndSwap(context.TODO(), key, body, body, true); err != nil {
					continue
				}
			} else {
				offset = offset + len(body) + 1
				newBody := maps.Clone(body)

				newBody[offset] = value
				if err := linKv.CompareAndSwap(context.TODO(), key, body, newBody, false); err != nil {
					continue
				}
			}
			return node.Reply(msg, map[string]interface{}{"type": "send_ok", "offset": offset})
		}

		return fmt.Errorf("Send: error")
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		input := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &input); err != nil {
			return fmt.Errorf("Poll: Error while decoding json: %s", err)
		}

		msgs := make(map[string][][2]int)
		for key, v := range input["offsets"].(map[string]interface{}) {
			body := make(map[int]float64)
			if err := linKv.ReadInto(context.TODO(), key, &body); maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				continue
				// return fmt.Errorf("Poll: Error while getting key %s : %s", key, err)
			}
			for offset, val := range body {
				if offset >= int(v.(float64)) {
					msgs[key] = append(msgs[key], [2]int{offset, int(val)})
				}
			}
		}

		for _, val := range msgs {
			slices.SortFunc(val, func(i, j [2]int) int {
				return i[0] - j[0]
			})
		}

		return node.Reply(msg, map[string]interface{}{"type": "poll_ok", "msgs": msgs})
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("COMMIT_OFFSETS: Error while decoding json: %s", err)
		}

		offsets := body["offsets"].(map[string]interface{})
		for key, offset := range offsets {
			for {
				val, err := seqKv.ReadInt(context.TODO(), key+"Commit")
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					if err := seqKv.CompareAndSwap(context.TODO(), key+"Commit", val, offset, true); err != nil {
						continue
					}
				} else if err != nil {
					return fmt.Errorf("commit_offsets: Could not fetch key %s details: %s", key, err)
				} else {
					if err := seqKv.CompareAndSwap(context.TODO(), key+"Commit", val, offset, false); err != nil {
						continue
					}
				}
				break
			}
		}

		return node.Reply(msg, map[string]interface{}{"type": "commit_offsets_ok"})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return fmt.Errorf("LIST_COMMITED_OFFSETS: Error while decoding json: %s", err)
		}

		keys := body["keys"].([]interface{})
		res := make(map[string]int)
		for _, key := range keys {
			val, err := seqKv.ReadInt(context.TODO(), key.(string)+"Commit")
			if err != nil {
				continue
			}
			res[key.(string)] = val
		}

		return node.Reply(msg, map[string]interface{}{"type": "list_committed_offsets_ok", "offsets": res})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
