package main

import (
	"fmt"
	"log"
	"time"
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

func ProcessedSectors() {
	//uniqueSectorIds := make(map[int]struct{})
	var selectedMessages []SectorResponse
	//taskId := 1
	processedMessages := 0
	timeoutCount := 0
	//round := 1
	countSectors := 10
	ch := make(chan int)

	// Горутина для отправки сообщений в канал с паузой 2 секунды
	go func() {
		for i := 0; i < 10; i++ { // Например, 10 сообщений
			ch <- i                     // Отправить сообщение в канал
			time.Sleep(2 * time.Second) // Пауза 2 секунды
			fmt.Println("Проверка секторов")
		}
		close(ch) // Закрыть канал после отправки всех сообщений
	}()

loop:
	for {
		select {
		case msg, ok := <-ch:
			processedMessages++
			if !ok {
				log.Println("Channel closed. Exiting the loop.")
				fmt.Println(msg)
				break loop
			}

			// Ваш код обработки сообщения
			// ...

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
	log.Println("Success sectors check, continue...")
}

func main() {
	ProcessedSectors()
	fmt.Println("Побежали сектора")
	Locations := make(map[string]int)
	Locations["132-222"] = 1
	Locations["132-223"] = 2
	Locations["132-224"] = 3
	for _, item := range Locations {
		fmt.Println(item)
	}
}
