package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/fujiwara/go-jsqlite"
	"github.com/pkg/profile"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}

func main() {
	defer profile.Start(profile.ProfilePath(".")).Stop()

	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run() error {
	var dsn string
	flag.StringVar(&dsn, "dsn", ":memory:", "go-sqlite3 data source name")
	flag.Parse()

	args := flag.Args()
	log.Println(args)
	if len(args) == 0 {
		flag.Usage()
		return nil
	}
	query := args[0]
	var f io.Reader
	if len(args) > 1 {
		var err error
		f, err = os.Open(args[1])
		if err != nil {
			return err
		}
	} else {
		f = os.Stdin
	}

	runner, err := jsqlite.New(dsn)
	if err != nil {
		return err
	}
	if err := runner.Read(f); err != nil {
		return err
	}
	rows, err := runner.Select(query)
	if err != nil {
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
