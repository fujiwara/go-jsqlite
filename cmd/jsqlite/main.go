package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/fujiwara/go-jsqlite"
	"github.com/pkg/profile"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}

func main() {
	if b, _ := strconv.ParseBool(os.Getenv("PPROF_ENABLED")); b {
		defer profile.Start(profile.ProfilePath(".")).Stop()
	}

	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run() error {
	var memory bool
	flag.BoolVar(&memory, "memory", false, "use in-memory database")
	flag.Parse()

	args := flag.Args()
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

	runner, err := jsqlite.New(memory)
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
