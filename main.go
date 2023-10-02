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

func Fill() {
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

	for i := 0; i < 10; i++ {
		msg := i
		fmt.Println("Sending sector", msg+1)
		response := SectorResponse{
			TaskID:     msg,
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

func ConsumeMessages(fch chan SectorResponse) {
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

	timeoutCount := 0
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

			if sectorResp.ReturnCode == 0 && sectorResp.RoundNo == 0 {
				fmt.Println("Ack in consumer func", sectorResp.SectorID)
				selectedMessages = append(selectedMessages, sectorResp)
				fch <- sectorResp
			} else {
				fmt.Println("Non-zero ReturnCode, skipping...")
			}

			if len(selectedMessages) == countSectors {
				fmt.Println("Received all sectors.")
				break loop
			}

		case <-time.After(10 * time.Second):
			if countSectors == len(selectedMessages) {
				break loop
			} else {
				timeoutCount++
				if timeoutCount == 10 {
					break loop
				}
			}

		}
	}
	close(fch)
}

func main() {
	// var foundItems []SectorResponse
	// var notFoundItems []SectorResponse
	Locations := make(map[string]int)
	Locations["132-221"] = 1
	Locations["132-222"] = 2
	Locations["132-223"] = 3
	Locations["132-224"] = 4
	Locations["132-225"] = 5
	Locations["132-226"] = 6
	Locations["132-227"] = 7
	Locations["132-228"] = 8
	Locations["132-229"] = 9
	Locations["132-230"] = 10
	Locations["132-231"] = 11
	Locations["132-232"] = 12
	// отправка в processed_sectors обработанных секторов
	go Fill()

	var receivedItems []SectorResponse
	// канал для получения данных из очереди
	fch := make(chan SectorResponse)

	// утилита для обработки очереди processed_sectors, после нее идет отправка  в бэк
	go ConsumeMessages(fch)

	// ожидаем получение всех данных из канала перед вызовом sendToCondResponse
	for item := range fch {
		fmt.Println("From rabbit sectorId", item.SectorID)
		receivedItems = append(receivedItems, item)
	}

	var differences []int

	// Преобразуем SectorID в строку для проверки наличия в Locations

	for _, sectorID := range Locations {
		found := false
		fmt.Println("Loc", sectorID)
		for _, item := range receivedItems {
			if item.SectorID == sectorID {
				found = true
				break
			}
		}
		if !found {
			differences = append(differences, sectorID)
		}
	}

	fmt.Println("Различия между Locations и receivedItems:", differences)
	fmt.Println("Start send to condional response")

	// после получения всех данных вызываем sendToCondResponse
	for _, sectorID := range Locations {
		fmt.Println("Location", sectorID)
	}
	// Ждем нажатия клавиши Enter перед завершением программы
	fmt.Println("Press Enter to exit")

}
