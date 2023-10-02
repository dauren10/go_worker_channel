package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type SectorResponse struct {
	TaskID     int
	RoundNo    int
	SectorID   int
	CellID     int
	LAC        int
	Service    string
	Timestamp  time.Time
	ReturnCode int
	Result     string
	Reason     string
}

func Fill(ch *amqp.Channel) {
	queueName := "processed_sectors"

	for i := 0; i < 10; i++ {
		msg := i
		fmt.Println("Sending sector", msg)
		response := SectorResponse{
			TaskID:     msg,
			RoundNo:    0,
			SectorID:   msg,
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

		time.Sleep(2 * time.Second)
	}
}

func ConsumeMessages(ch *amqp.Channel) {
	queueName := "processed_sectors"
	msgs, err := ch.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	timeout := time.After(30 * time.Second)
	countSectors := 10
	selectedMessages := make([]SectorResponse, 0)
loop:
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				log.Println("Channel closed. Exiting the loop.")
				return
			}

			var sectorResp SectorResponse
			err := json.Unmarshal(msg.Body, &sectorResp)
			if err != nil {
				log.Println("Failed to unmarshal JSON:", err)
				continue
			}

			if sectorResp.ReturnCode == 0 {
				fmt.Println("Ack")
				selectedMessages = append(selectedMessages, sectorResp)
			}

			if len(selectedMessages) == countSectors {
				fmt.Println("Received all sectors.")
				break loop
			}

		case <-timeout:
			fmt.Println("Timed out. Exiting the loop.")
			break loop
		}
	}
}

func main() {
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

	go Fill(ch)
	ConsumeMessages(ch)

	// Ждем нажатия клавиши Enter перед завершением программы
	fmt.Println("Press Enter to exit")
	fmt.Scanln()
}
