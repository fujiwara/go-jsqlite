package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/fujiwara/go-jsqlite"
)

func main() {
	err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()
	if len(os.Args) < 1 {
		flag.Usage()
		return nil
	}
	query := os.Args[1]
	var f io.Reader
	if len(os.Args) > 2 {
		var err error
		f, err = os.Open(os.Args[2])
		if err != nil {
			return err
		}
	} else {
		f = os.Stdin
	}

	runner, err := jsqlite.Read(f)
	if err != nil {
		return err
	}
	rows, err := runner.Select(query)
	if err != nil && !jsqlite.NoSuchColumnError(err) {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	enc := json.NewEncoder(os.Stdout)
	for _, row := range rows {
		enc.Encode(row)
	}
	return nil
}
