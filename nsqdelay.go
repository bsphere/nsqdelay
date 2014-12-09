package main

import (
	_ "code.google.com/p/gosqlite/sqlite3"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"github.com/augurysys/timestamp"
	nsq "github.com/bitly/go-nsq"
	"log"
	"sync"
	"time"
)

type DelayedMessage struct {
	Topic  string              `json:"topic"`
	Body   string              `json:"body"`
	SendAt timestamp.Timestamp `json:"send_at"`
}

type message struct {
	Id     string
	Topic  string
	Body   []byte
	SendAt timestamp.Timestamp
}

var db *sql.DB

var insert chan *message

func main() {
	// parse command line arguments
	var lookupd, topic, nsqd, dbpath string

	flag.StringVar(&lookupd, "lookupd_http_address", "http://127.0.0.1:4161",
		"lookupd HTTP address")

	flag.StringVar(&nsqd, "nsqd_tcp_address", "127.0.0.1:4150",
		"nsqd TCP address")

	flag.StringVar(&topic, "topic", "delayed",
		"NSQD topic for delayed messages")

	flag.StringVar(&dbpath, "db", "/data/db.dat", "database file path")
	flag.Parse()

	if lookupd == "" || topic == "" || nsqd == "" || dbpath == "" {
		flag.PrintDefaults()
		log.Fatal("invalid arguments")
	}

	// initialize the sqlite3 database
	var err error
	db, err = sql.Open("sqlite3", dbpath)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS messages(id TEXT, send_at INTEGER, topic TEXT, body BLOB);
		CREATE UNIQUE INDEX ON messages(id);`); err != nil {

		log.Fatal(err)
	}

	// initialize channels
	insert = make(chan *message)

	// initialize a consumer for delayed messages
	c, err := nsq.NewConsumer(topic, "scheduler", nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}

	// register an handler for incoming messages
	c.AddHandler(nsq.HandlerFunc(messageHandler))

	if err := c.ConnectToNSQLookupd(lookupd); err != nil {
		log.Fatal(err)
	}

	// initialize a producer
	p, err := nsq.NewProducer(nsqd, nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}

	// handle sqlite insert, query and remove in a goroutine
	go func() {
		for {
			select {
			case m := <-insert:
				if _, err := db.Exec(`
					REPLACE INTO messages(id, send_at, topic, body) VALUES(?, ?, ?, ?);`,
					m.Id, m.SendAt.Unix(), m.Topic, m.Body); err != nil {

					log.Print(err)
				}

			default:
				func() {
					now := time.Now().Unix()

					rows, err := db.Query(
						"SELECT id, topic, body from messages where send_at<?",
						now)

					if err != nil {
						log.Print(err)
						return
					}

					defer rows.Close()

					var del []*message
					defer func() {
						for _, d := range del {
							// remove the message from sqlite
							if _, err := db.Exec(`
								DELETE from messages WHERE id=?`, d.Id); err != nil {

								log.Print(err)
							}
						}
					}()

					for rows.Next() {
						var m message

						if err := rows.Scan(&m.Id, &m.Topic, &m.Body); err != nil {
							log.Print(err)
							return
						}

						// publish the message
						err := p.Publish(m.Topic, m.Body)
						if err != nil {
							log.Print(err)
							return
						}

						log.Printf("published message '%s' to topic '%s'", m.Id, m.Topic)

						// mark the message for deletion
						del = append(del, &m)
					}

					if rows.Err() != nil {
						log.Print(err)
						return
					}

					time.Sleep(time.Second)
				}()
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}

func messageHandler(m *nsq.Message) error {
	defer m.Finish()
	var d DelayedMessage

	if err := json.Unmarshal(m.Body, &d); err != nil {
		log.Print(err)
		return err
	}

	// data validation
	if d.Topic == "" || d.Body == "" {
		log.Print("invalid delayed message data")
		return errors.New("invalid delayed message data")
	}

	// insert the message to sqlite
	ms := &message{
		Id:     string(m.ID[:nsq.MsgIDLength]),
		Topic:  d.Topic,
		Body:   []byte(d.Body),
		SendAt: d.SendAt,
	}

	insert <- ms

	return nil
}
