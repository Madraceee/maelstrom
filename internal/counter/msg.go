package counter

type input struct {
	Delta int `json:"delta"`
}

type simpleOutput struct {
	Type  string `json:"type"`
}

type output struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

