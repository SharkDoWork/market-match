# Usage
#
# make tag=relase-tag // to build linux-platform binary with tag
# make mac tag=relase-tag // to build darwin-platform binary with tag
#
# This is how we want to name the binary output
TPATH=${path}
APP=exchange
OUTPUT=${TPATH}exchange.bin

# These are the values we want to pass for Version and BuildTime
GITTAG=${tag}
BUILD_TIME=`date +%FT%T%z`
HOSTNAME=`hostname`
REPOSITORY=

# Setup the -ldflags option for go build here, interpolate the variable values
LDFLAGS=-ldflags "-X main.GitTag=${GITTAG}-${HOSTNAME} -X main.BuildTime=${BUILD_TIME}"

all:
	docker run --rm -v `pwd`:/go/src/match-engine -w /go/src/match-engine  golang:1.11.2 sh -c 'CGO_ENABLED=0 go build -a -installsuffix cgo ${LDFLAGS} -o ${OUTPUT} main/exchange.go'
mac:
	go build ${LDFLAGS} -o ${OUTPUT} main/exchange.go
test:
	go  test -count=1 ./match/ ./snapshotter/ ./puller/ ./redis/ ./scheduler ./validate ./market ./rabbitmq
docker: all
	docker build -t exchange:${GITTAG} .
docker-release: docker
	docker image tag ${APP}:${GITTAG} ${REPOSITORY}/${APP}:${GITTAG}
	docker push ${REPOSITORY}/${APP}:${GITTAG}
