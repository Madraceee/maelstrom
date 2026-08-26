package transaction

type input struct {
	Txns []any `json:"txn"`
}

type output struct {
	Type string `json:"type"`
	Txn  []any  `json:"txn"`
}

type inputChanMsg struct {
	input
	shouldBroadcast bool
	src string
	ch chan []any 
}

type errorMsg struct {
	Type string `json:"type"`
	Code int    `json:"code"`
	Text string `json:"text"`
}

func newError(code int) errorMsg {
	return errorMsg{
		Type: "error",
		Code: code,
		Text: "txn abort",
	}
}
