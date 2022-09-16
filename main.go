package main

import (
	"encoding/json"
	"github.com/fahrizalfarid/simple-go-rabbit/communication"
	"sync"
	"time"
)

type data struct {
	Num int
}

func main() {
	wg := sync.WaitGroup{}

	// 100 consumer
	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			runConsumer()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		runPublish(10)
	}()

	wg.Wait()
}

func runPublish(n int) {
	conn := communication.NewConnection(
		&communication.Config{
			// master-node
			Uri:            "amqp://user:pass@localhost:5672/",
			Queuename:      "kijang-1",
			TimeOutContext: 1 * time.Second,
		},
	)

	err := conn.Connect()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	err = conn.BindQueue()
	if err != nil {
		panic(err)
	}

	for i := 0; i < n; i++ {
		msg := data{
			Num: i,
		}

		msgEnc, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}

		err = conn.Publish(msgEnc)
		if err != nil {
			panic(err)
		}
	}

}

func runConsumer() {
	conn := communication.NewConnection(
		&communication.Config{
			// node-0
			Uri:            "amqp://user:pass@localhost:5673/",
			Queuename:      "kijang-1",
			TimeOutContext: 1 * time.Second,
		},
	)

	err := conn.Connect()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	err = conn.BindQueue()
	if err != nil {
		panic(err)
	}

	conn.RunConsumer()
}
