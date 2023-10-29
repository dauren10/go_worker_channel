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

func worker(task_id int, ch <-chan amqp.Delivery, wg *sync.WaitGroup, taskData map[int][]SectorResponse) {
	defer wg.Done()
	fmt.Printf("Worker %d started\n", task_id)
	lenSectorTaskId := 0
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				fmt.Printf("Worker %d: Channel closed, exiting\n", task_id)

			}

			var sectorResp SectorResponse
			err := json.Unmarshal(msg.Body, &sectorResp)
			if err != nil {
				log.Println("Failed to unmarshal JSON:", err)
				msg.Ack(false) // Подтверждаем сообщение в случае ошибки при разборе JSON
				continue
			}

			msg.Ack(false)
			taskData[sectorResp.TaskID] = append(taskData[sectorResp.TaskID], sectorResp)
			lenSectorTaskId = len(taskData[sectorResp.TaskID])
			if lenSectorTaskId == 10 {
				fmt.Println(lenSectorTaskId, sectorResp.TaskID)
				fmt.Println("Stop worker break")
				break
			}
			time.Sleep(time.Second * 1)

		}
	}

}

func consumeMessages(messages <-chan amqp.Delivery, workerChannel chan<- amqp.Delivery) {
	for msg := range messages {
		workerChannel <- msg
	}
	close(workerChannel)
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

func main() {
	numWorkers := 3

	conn, ch, messages, err := setupRabbitMQ()
	if err != nil {
		log.Fatalf("Error setting up RabbitMQ: %v", err)
	}
	defer ch.Close()
	defer conn.Close()
	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("Error setting QoS: %v", err)
	}

	var wg sync.WaitGroup
	workerChannel := make(chan amqp.Delivery)

	go consumeMessages(messages, workerChannel)
	taskData := make(map[int][]SectorResponse)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, workerChannel, &wg, taskData)
	}

	wg.Wait()
	fmt.Println("All workers are done.")
}
