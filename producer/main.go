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
	TaskID     int       `json:"task_id"`
	RoundNo    int       `json:"round_no"`
	SectorID   int       `json:"sector_id"`
	CellID     int       `json:"cell_id"`
	LAC        int       `json:"lac"`
	Service    string    `json:"service"`
	Timestamp  time.Time `json:"timestamp"`
	ReturnCode int       `json:"return_code"`
	Result     string    `json:"result"`
	Reason     string    `json:"reason"`
}

func Fill(wg *sync.WaitGroup) {
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
	queueName := "processed_sectors"

	for i := 0; i < 5; i++ {
		msg := i
		fmt.Println("Sending sector", msg+1)
		response := SectorResponse{
			TaskID:     1,
			RoundNo:    0,
			SectorID:   msg + 1,
			CellID:     msg,
			LAC:        msg,
			Service:    "service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
			Result:     "result",
			Reason:     "reason",
		}

		jsonBody, err := json.Marshal(response)
		if err != nil {
			fmt.Println("Failed to marshal JSON:", err)
			return
		}

		err = ch.Publish(
			"",
			queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonBody,
			})
		if err != nil {
			fmt.Println("Failed to publish a message:", err)
			return
		}

		time.Sleep(1 * time.Second)
	}

	for i := 0; i < 5; i++ {
		msg := i
		fmt.Println("Sending sector", msg+1)
		response := SectorResponse{
			TaskID:     2,
			RoundNo:    0,
			SectorID:   msg + 1,
			CellID:     msg,
			LAC:        msg,
			Service:    "service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
			Result:     "result",
			Reason:     "reason",
		}

		jsonBody, err := json.Marshal(response)
		if err != nil {
			fmt.Println("Failed to marshal JSON:", err)
			return
		}

		err = ch.Publish(
			"",
			queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonBody,
			})
		if err != nil {
			fmt.Println("Failed to publish a message:", err)
			return
		}

		time.Sleep(1 * time.Second)
	}

	for i := 0; i < 5; i++ {
		msg := i
		fmt.Println("Sending sector", msg+1)
		response := SectorResponse{
			TaskID:     3,
			RoundNo:    0,
			SectorID:   msg + 1,
			CellID:     msg,
			LAC:        msg,
			Service:    "service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
			Result:     "result",
			Reason:     "reason",
		}

		jsonBody, err := json.Marshal(response)
		if err != nil {
			fmt.Println("Failed to marshal JSON:", err)
			return
		}

		err = ch.Publish(
			"",
			queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonBody,
			})
		if err != nil {
			fmt.Println("Failed to publish a message:", err)
			return
		}

		time.Sleep(1 * time.Second)
	}

	for i := 0; i < 5; i++ {
		msg := i
		fmt.Println("Sending sector", msg+1)
		response := SectorResponse{
			TaskID:     1,
			RoundNo:    0,
			SectorID:   msg + 1,
			CellID:     msg,
			LAC:        msg,
			Service:    "service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
			Result:     "result",
			Reason:     "reason",
		}

		jsonBody, err := json.Marshal(response)
		if err != nil {
			fmt.Println("Failed to marshal JSON:", err)
			return
		}

		err = ch.Publish(
			"",
			queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonBody,
			})
		if err != nil {
			fmt.Println("Failed to publish a message:", err)
			return
		}

		time.Sleep(1 * time.Second)
	}

	for i := 0; i < 5; i++ {
		msg := i
		fmt.Println("Sending sector", msg+1)
		response := SectorResponse{
			TaskID:     2,
			RoundNo:    0,
			SectorID:   msg + 1,
			CellID:     msg,
			LAC:        msg,
			Service:    "service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
			Result:     "result",
			Reason:     "reason",
		}

		jsonBody, err := json.Marshal(response)
		if err != nil {
			fmt.Println("Failed to marshal JSON:", err)
			return
		}

		err = ch.Publish(
			"",
			queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonBody,
			})
		if err != nil {
			fmt.Println("Failed to publish a message:", err)
			return
		}

		time.Sleep(1 * time.Second)
	}

	for i := 0; i < 5; i++ {
		msg := i
		fmt.Println("Sending sector", msg+1)
		response := SectorResponse{
			TaskID:     3,
			RoundNo:    0,
			SectorID:   msg + 1,
			CellID:     msg,
			LAC:        msg,
			Service:    "service",
			Timestamp:  time.Now(),
			ReturnCode: 0,
			Result:     "result",
			Reason:     "reason",
		}

		jsonBody, err := json.Marshal(response)
		if err != nil {
			fmt.Println("Failed to marshal JSON:", err)
			return
		}

		err = ch.Publish(
			"",
			queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonBody,
			})
		if err != nil {
			fmt.Println("Failed to publish a message:", err)
			return
		}

		time.Sleep(1 * time.Second)
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	go Fill(&wg)
	wg.Wait()
}
