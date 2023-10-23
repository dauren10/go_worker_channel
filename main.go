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

func worker(id int, ch <-chan amqp.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Worker %d started\n", id)
	for msg := range ch {
		var sectorResp SectorResponse
		err := json.Unmarshal(msg.Body, &sectorResp)
		if err != nil {
			log.Println("Failed to unmarshal JSON:", err)
			msg.Ack(false)
			continue
		}
		fmt.Printf("Worker %d received message: %s\n", id, msg.Body)
		// Process the message here
		time.Sleep(time.Second * 1) // Simulating work with sleep
		msg.Ack(false)
	}
	fmt.Printf("Worker %d finished\n", id)
}

func setupRabbitMQ() (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery, error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("Failed to open a channel: %v", err)
	}

	q, err := ch.QueueDeclare(
		"processed_sectors",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, nil, fmt.Errorf("Failed to declare a queue: %v", err)
	}

	messages, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, nil, fmt.Errorf("Failed to register a consumer: %v", err)
	}

	return conn, ch, messages, nil
}

func processMessages(messages <-chan amqp.Delivery, numWorkers, bufferSize int) {
	var wg sync.WaitGroup

	workerChannel := make(chan amqp.Delivery, bufferSize)

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, workerChannel, &wg)
	}

	go func() {
		for msg := range messages {
			workerChannel <- msg
		}
		close(workerChannel)
	}()

	wg.Wait()
	fmt.Println("All workers are done.")
}

func main() {
	numWorkers := 3
	bufferSize := 10

	conn, ch, messages, err := setupRabbitMQ()
	if err != nil {
		log.Fatalf("Error setting up RabbitMQ: %v", err)
	}
	defer ch.Close()
	defer conn.Close()

	processMessages(messages, numWorkers, bufferSize)
}
