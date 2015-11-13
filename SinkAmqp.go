// This example declares a durable Exchange, an ephemeral (auto-delete) Queue,
// binds the Queue to the Exchange with a binding key, and consumes every
// message published to that Exchange with that routing key.
//
package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	//"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var f *os.File

var w *bufio.Writer
var (
	uri          = flag.String("uri", "amqp://fx:fx@localhost:5672/fx", "AMQP URI")
	exchange     = flag.String("exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	queue        = flag.String("queue", "live.quote2", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("key", "test-key", "AMQP binding key")
	consumerTag  = flag.String("consumer-tag", "simple-consumer", "AMQP consumer tag (should not be blank)")
	lifetime     = flag.Duration("lifetime", 0*time.Second, "lifetime of process before shutdown (0s=infinite)")
)

func init() {
	flag.Parse()
}

func main() {
	c, err := NewConsumer(*uri, *queue, *consumerTag)
	if err != nil {
		log.Fatalf("%s", err)
	}

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		log.Printf("Processing signal !")

		done <- true
	}()

	if *lifetime > 0 {
		log.Printf("running for %s", *lifetime)
		time.Sleep(*lifetime)
	} else {
		log.Printf("running forever")
		<-done
	}

	log.Printf("shutting down")

	if err := c.Shutdown(); err != nil {
		log.Fatalf("error during shutdown: %s", err)
	}

}

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func NewConsumer(amqpURI, queueName, ctag string) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
	}

	var err error

	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	/*
		log.Printf("got Channel, declaring Exchange (%q)", exchange)
		if err = c.channel.ExchangeDeclare(
			exchange,     // name of the exchange
			exchangeType, // type
			true,         // durable
			false,        // delete when complete
			false,        // internal
			false,        // noWait
			nil,          // arguments
		); err != nil {
			return nil, fmt.Errorf("Exchange Declare: %s", err)
		}

		log.Printf("declared Exchange, declaring Queue %q", queueName)
	*/

	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	/*
		log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
			queue.Name, queue.Messages, queue.Consumers, key)

		if err = c.channel.QueueBind(
			queue.Name, // name of the queue
			key,        // bindingKey
			exchange,   // sourceExchange
			false,      // noWait
			nil,        // arguments
		); err != nil {
			return nil, fmt.Errorf("Queue Bind: %s", err)
		}
	*/
	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
	deliveries, err := c.channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	go handle(deliveries, c.done)

	return c, nil
}

func (c *Consumer) Shutdown() error {

	if f != nil {
		f.Sync()
		f.Close()
	}

	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	tyear := -1
	tmonth := time.January
	tday := -1
	startingTime := time.Now().UTC()
	for d := range deliveries {
		/*
			log.Printf(
				"got %dB delivery: [%v] %q",
				len(d.Body),
				d.DeliveryTag,
				d.Body,
			)
		*/
		r := csv.NewReader(strings.NewReader(string(d.Body)))
		r.Comma = ';'

		records, err := r.ReadAll()
		if err != nil {
			log.Fatal(err)
		}

		datetimestring := records[0][2]
		//log.Println(datetimestring)
		const lt = "2006.1.2 15:04:05"
		t, _ := time.Parse(lt, datetimestring)
		tyearnew, tmonthnew, tdaynew := t.Date()

		if tday != tdaynew || tmonth != tmonthnew || tyear != tyearnew {
			tyear = tyearnew
			tmonth = tmonthnew
			tday = tdaynew

			var buffer bytes.Buffer
			buffer.WriteString("fx")
			buffer.WriteString(strconv.Itoa(tyear))
			buffer.WriteString(strconv.Itoa(int(tmonth)))
			buffer.WriteString(strconv.Itoa(tday))
			buffer.WriteString(".csv")
			filename := buffer.String()

			if f != nil {
				f.Sync()
				f.Close()
			}

			f, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				log.Println(err)

				f, err = os.Create(filename)
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("Created file:" + filename)
			}

			defer f.Close()
			w = bufio.NewWriter(f)
		}

		w.Write(d.Body)
		w.WriteString("\n")

		endingTime := time.Now().UTC()
		var duration time.Duration = endingTime.Sub(startingTime)

		if duration > 1*time.Second {
			//			f.Sync()
			w.Flush()
			w = bufio.NewWriter(f)
			//runtime.GC()
			//runtime.FreeOSMemory()
		}

		//fmt.Println(t.Date())

		d.Ack(false)
	}
	log.Printf("handle: deliveries channel closed")
	done <- nil
}
