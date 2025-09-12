#!/usr/bin/env bash
docker run --rm -v "$PWD":/go/src/match-engine -w /go/src/match-engine  golang:1.11.2 go build main/match-engine.go
#docker run --rm -v "$PWD":/go/src/match-engine -w /go/src/match-engine  golang:1.11.2 go run main/exchange.go

