FROM golang:1.3.3

# install libsqlite3-dev
RUN echo "deb http://ftp.debian.org/debian jessie-backports main contrib non-free" >> /etc/apt/sources.list
RUN apt-get update; apt-get install -y libsqlite3-dev

# install godep
RUN go get github.com/tools/godep

# copy source code
ADD . /go/src/github.com/bsphere/nsqdelay

# install godep dependencies
WORKDIR /go/src/github.com/bsphere/nsqdelay

RUN godep restore

WORKDIR /go

# build and install the source code
RUN go install github.com/bsphere/nsqdelay

VOLUME ["/data"]

ENTRYPOINT ["/go/bin/nsqdelay"]
