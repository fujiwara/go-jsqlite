package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/fujiwara/go-jsqlite"
)

func main() {
	q := os.Args[1]
	runner, _ := jsqlite.Read(os.Stdin)
	rows, err := runner.Select(q)
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(os.Stdout)
	for _, row := range rows {
		enc.Encode(row)
	}
}
