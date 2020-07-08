jsqlite: *.go cmd/jsqlite/*.go go.*
	go build --tags "sqlite_json" -o jsqlite cmd/jsqlite/main.go

test:
	go test --tags "sqlite_json" ./...

install: jsqlite
	install jsqlite $(GOPATH)/bin

clean:
	rm -f jsqlite
