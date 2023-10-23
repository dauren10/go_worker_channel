package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type SectorResponse struct {
	TaskID     int    `json:"task_id"`
	RoundNo    int    `json:"round_no"`
	SectorID   int    `json:"sector_id"`
	CellID     int    `json:"cell_id"`
	LAC        int    `json:"lac"`
	Service    string `json:"service"`
	Timestamp  string `json:"timestamp"`
	ReturnCode int    `json:"return_code"`
	Result     string `json:"result"`
	Reason     string `json:"reason"`
}

func worker(task_id int, fch chan SectorResponse, done chan struct{}) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"processed_sectors",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	fmt.Printf("Worker %d started\n", task_id)

	for msg := range msgs {
		var sectorResp SectorResponse
		err := json.Unmarshal(msg.Body, &sectorResp)
		if err != nil {
			log.Println("Failed to unmarshal JSON:", err)
			msg.Ack(false)
			continue
		}
		fmt.Println(task_id, sectorResp.TaskID)
		fmt.Println(sectorResp)
		if sectorResp.TaskID > 0 {
			msg.Ack(false)
			fmt.Printf("Worker %d received message: %s\n", task_id, msg.Body)
			fch <- sectorResp
			time.Sleep(time.Second * 1)
		}
	}

	done <- struct{}{}
}

func main() {
	bufferSize := 10
	var receivedItems []SectorResponse
	fch := make(chan SectorResponse, bufferSize)
	done := make(chan struct{})
	var wg sync.WaitGroup

	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go worker(i, fch, done)
	}

	go func() {
		wg.Wait()
		close(fch)
		close(done)
	}()

	for {
		select {
		case item, ok := <-fch:
			if !ok {
				fch = nil // закрыть канал fch
				break
			}
			fmt.Println("From rabbit sectorId", item)
			receivedItems = append(receivedItems, item)
		case <-done:
			wg.Done()
		}

	}

	<-make(chan struct{})
}
