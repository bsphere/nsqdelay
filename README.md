nsqdelay - a delayed message queue for NSQ
==========================================

[![Build Status](https://travis-ci.org/bsphere/nsqdelay.svg?branch=master)](https://travis-ci.org/bsphere/nsqdelay)

__nsqdelay__ can be used for sending delayed messages on top of NSQ,
it listens on the __delayed__ topic by default (configurable) and receives JSON encoded messages with the following structure:

```
{
  "topic": "my_topic",
  "body": "message_body",
  "send_at": <unix timestamp>
}
```

It persists the messages to an sqlite file database and publish them when the time comes.

Usage
-----
For all command line arguments use `docker run --rm gbenhaim/nsqdelay -h`

Otherwise run it with `docker run -d --name nsqdelay gbenhaim/nsqdelay ....`

- it is advisable to mount the path of the sqlite db file as a volume, so delayed messages will be persisted.
