package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"

	"sql_evaluator/sqleval"
)

func usageString() string {
	return fmt.Sprintf("Usage: %s <table-folder> <sql-json-file> <output-file>\n", os.Args[0])
}

func die(message string) {
	os.Stderr.WriteString(message)
	os.Exit(1)
}

func main() {
	if len(os.Args) != 4 {
		die(usageString())
	}

	tableFolder := os.Args[1]
	sqlJsonFile := os.Args[2]
	outputFile := os.Args[3]

	var query sqleval.Query
	{
		reader, err := os.Open(sqlJsonFile)
		if err != nil {
			die(fmt.Sprintf("Error opening %q: %s\n", sqlJsonFile, err))
		}
		defer reader.Close()

		err = json.NewDecoder(reader).Decode(&query)
		if err != nil {
			die(fmt.Sprintf("Error reading %q as query JSON: %s\n", sqlJsonFile, err))
		}
	}

	var tables []sqleval.Table = nil
	for _, tableRef := range query.From {
		tableJsonFile := path.Join(tableFolder, tableRef.Source+".table.json")
		reader, err := os.Open(tableJsonFile)
		if err != nil {
			die(fmt.Sprintf("Error opening %q: %s\n", tableJsonFile, err))
		}
		defer reader.Close()

		var table sqleval.Table
		err = json.NewDecoder(reader).Decode(&table)
		if err != nil {
			die(fmt.Sprintf("Error reading %q as table JSON: %s\n", tableJsonFile, err))
		}
		tables = append(tables, table)
	}

	// TODO: Actually evaluate query.
	// For now, just dump the input back out.
	{
		fw, err := os.Create(outputFile)
		if err != nil {
			die(fmt.Sprintf("Error opening %q for writing: %s\n", outputFile, err))
		}
		defer fw.Close()

		w := bufio.NewWriter(fw)
		defer w.Flush()

		mustJsonMarshalIndent(w, query)

		for _, table := range tables {
			w.WriteString("[\n")

			w.WriteString("    ")
			mustJsonMarshal(w, table.Columns)

			for _, row := range table.Rows {
				w.WriteString(",\n    ")
				mustJsonMarshal(w, row)
			}

			w.WriteString("\n]\n")
		}
	}
}

func newJsonEncoder(w io.Writer) *json.Encoder {
	encoder := json.NewEncoder(w)
	// By default, "&", "<", and ">" are escaped, which makes the output harder to read.  Disable that.
	encoder.SetEscapeHTML(false)
	return encoder
}

func mustJsonMarshal(w io.Writer, v interface{}) {
	buffer := &bytes.Buffer{}
	encoder := newJsonEncoder(buffer)
	err := encoder.Encode(v)
	if err != nil {
		panic(fmt.Sprintf("Can't happen: value should remarshal, but got error: %v", err))
	}
	// Ugh, Encode() adds a newline even though Marshal() does not.  Remove it.
	raw := buffer.Bytes()
	end := len(raw) - 1
	if raw[end] != '\n' {
		panic("Last byte written by Encode() wasn't a newline.")
	}
	raw = raw[:end]
	w.Write(raw)
}

func mustJsonMarshalIndent(w io.Writer, v interface{}) {
	encoder := newJsonEncoder(w)
	encoder.SetIndent("", "    ")
	err := encoder.Encode(v)
	if err != nil {
		panic(fmt.Sprintf("Can't happen: value should remarshal, but got error: %v", err))
	}
}
