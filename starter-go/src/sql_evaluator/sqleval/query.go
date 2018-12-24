package sqleval

// This file contains code for marshaling & unmarshaling "JSON-formatted SQL".

import (
	"encoding/json"
	"fmt"
)

type Query struct {
	Select []Selector   `json:"select"`
	From   []TableRef   `json:"from"`
	Where  []Comparison `json:"where"`
}

type Selector struct {
	Column ColumnRef `json:"column"`
	As     string    `json:"as"`
}

type ColumnRef struct {
	Name  string  `json:"name"`
	Table *string `json:"table"`
}

type TableRef struct {
	Source string `json:"source"`
	As     string `json:"as"`
}

type Comparison struct {
	Op    string `json:"op"`
	Left  Term   `json:"left"`
	Right Term   `json:"right"`
}

type Term struct {
	Column  *ColumnRef `json:"column,omitempty"`
	Literal *Value     `json:"literal,omitempty"`
}

// Code below this line handles the details of JSON marshaling.  You can probably ignore this.

func (t *Term) UnmarshalJSON(b []byte) error {
	var v interface{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	var result Term
	switch x := v.(type) {
	case map[string]interface{}:
		// Note: for simplicity we just pass the entire serialized form
		// and reparse in the helper unmarshal* functions; this makes
		// the code more straightforward than using reflection but has
		// some overhead.
		if _, ok := x["column"]; ok {
			err = unmarshalColumnTerm(b, &result)
		} else if _, ok := x["literal"]; ok {
			err = unmarshalLiteralTerm(b, &result)
		} else {
			err = fmt.Errorf("Invalid term in query (no column or literal): %q", b)
		}
	default:
		err = fmt.Errorf("Invalid term in query: %q", b)
	}
	if err != nil {
		return err
	}
	*t = result
	return nil
}

func unmarshalColumnTerm(b []byte, t *Term) error {
	var v struct {
		Column ColumnRef `json:"column"`
	}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	t.Column = &v.Column
	return nil
}

func unmarshalLiteralTerm(b []byte, t *Term) error {
	var unmarshaled struct {
		Literal interface{} `json:"literal"`
	}
	err := json.Unmarshal(b, &unmarshaled)
	if err != nil {
		return err
	}
	var literal Value
	switch x := unmarshaled.Literal.(type) {
	case string:
		literal.Str = &x
	case float64:
		i := int64(x) // We are intentionally oblivious to truncation here.
		literal.Int = &i
	default:
		return fmt.Errorf("Insupported literal type: %q", b)
	}
	t.Literal = &literal
	return nil
}
