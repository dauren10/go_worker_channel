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

func consumeMessages(messages <-chan amqp.Delivery, workerChannel chan<- amqp.Delivery) {
	for msg := range messages {
		workerChannel <- msg
	}

}
func processedSectors(taskID int, ch <-chan amqp.Delivery, taskData map[int][]SectorResponse, resultChannel chan<- SectorResponse) {

loop:
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				fmt.Println("Channel closed, exiting processedSectors")
				return
			}

			var sectorResp SectorResponse
			err := json.Unmarshal(msg.Body, &sectorResp)
			if err != nil {
				log.Println("Failed to unmarshal JSON:", err)
				msg.Ack(false)
				continue
			}

			msg.Ack(false)
			taskData[sectorResp.TaskID] = append(taskData[sectorResp.TaskID], sectorResp)

			lenSectorTaskID := len(taskData[sectorResp.TaskID])
			resultChannel <- sectorResp
			if lenSectorTaskID == 10 {

				fmt.Println("Stop processedSectors break", sectorResp.TaskID)
				break loop
			}

			time.Sleep(time.Second * 1)
		}
	}
}

func worker(taskID int, ch <-chan amqp.Delivery, taskData map[int][]SectorResponse, resultChannel chan<- SectorResponse, wg *sync.WaitGroup) {
	defer wg.Done() // Decrement the counter when the goroutine completes
	fmt.Printf("Worker %d started\n", taskID)
	processedSectors(taskID, ch, taskData, resultChannel)
	fmt.Println("Start send to condresponse")
}

func main() {
	numWorkers := 3
	conn, ch, messages, err := setupRabbitMQ()
	if err != nil {
		log.Fatalf("Error setting up RabbitMQ: %v", err)
	}

	defer ch.Close()
	defer conn.Close()

	var wg sync.WaitGroup
	workerChannel := make(chan amqp.Delivery)
	resultChannel := make(chan SectorResponse)

	go consumeMessages(messages, workerChannel)
	taskData := make(map[int][]SectorResponse)

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1) // Increment the counter before starting a goroutine
		go worker(i, workerChannel, taskData, resultChannel, &wg)
	}

	go func() {
		wg.Wait() // Wait for all workers to complete
		close(resultChannel)
	}()

	// Collect results from workers
	//finalResult := make(map[int][]SectorResponse)
	Locations := make(map[string]int)
	Locations["31"] = 31
	Locations["32"] = 32
	Locations["33"] = 33
	Locations["34"] = 34
	Locations["35"] = 35

	Locations["36"] = 36
	Locations["37"] = 37
	Locations["38"] = 38
	Locations["39"] = 39
	Locations["40"] = 40

	Locations["41"] = 41
	Locations["42"] = 42
	Locations["43"] = 43
	Locations["44"] = 44
	Locations["45"] = 45
	var receivedItems []SectorResponse
	for updatedData := range resultChannel {
		if updatedData.TaskID == 1 {
			receivedItems = append(receivedItems, updatedData)
			fmt.Println(updatedData)
		}

	}

	for _, sectorID := range Locations {
		found := false
		for _, item := range receivedItems {
			if sectorID == item.SectorID {
				found = true
				break
			}
		}
		if !found {
			fmt.Println(sectorID)
		}
	}

	fmt.Println("Final.......")

	fmt.Println("All workers are done.")

	// Use finalResult as needed
}
