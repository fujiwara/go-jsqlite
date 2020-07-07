# go-jsqlite

SQL query runner for JSONL.

[![GoDoc](https://godoc.org/github.com/fujiwara/go-jsqlite/validator?status.svg)](https://godoc.org/github.com/fujiwara/go-jsqlite/validator)

jsqlite uses [go-sqlite3](https://github.com/mattn/go-sqlite3) internal.

## Usage

### jsqlite command

```console
$ jsqlite "SELECT tag, time, message FROM records WHERE message LIKE '%error%' AND tag LIKE 'foo.%'" tests/logs.json
{"message":"[ERROR] hoge","tag":"foo.bar","time":"2020-07-06T17:49:37+0900"}
{"message":"[ERROR] hoge baz","tag":"foo.baz","time":"2020-07-06T17:49:43+0900"}
```

## LICENSE

MIT
