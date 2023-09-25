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
	FinishedLocations := make(map[string]int)

	// Горутина для отправки сообщений в канал с паузой 2 секунды
	go func() {
		for i := 0; i < 10; i++ {
			ch <- i
			time.Sleep(2 * time.Second)

		}
		close(ch)
	}()

loop:
	for {
		select {

		case msg, ok := <-ch:
			processedMessages++
			fmt.Println("Проверка секторов")
			if !ok {
				log.Println("Channel closed. Exiting the loop.")

				break loop
			} else {
				if msg == 0 {
					FinishedLocations["132-221"] = 1
				} else if msg == 1 {
					FinishedLocations["132-222"] = 2
				} else if msg == 2 {
					FinishedLocations["132-223"] = 3
				} else if msg == 3 {
					FinishedLocations["132-224"] = 4
				} else if msg == 4 {
					FinishedLocations["132-225"] = 5
				} else if msg == 5 {
					FinishedLocations["132-226"] = 6
				} else if msg == 6 {
					FinishedLocations["132-227"] = 7
				} else if msg == 7 {
					FinishedLocations["132-228"] = 8
				} else if msg == 8 {
					FinishedLocations["132-229"] = 9
				} else if msg == 9 {
					FinishedLocations["132-230"] = 10
				}
				fmt.Println(msg)
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
	fmt.Println(FinishedLocations)
	log.Println("Success sectors check, continue...")
}

func main() {
	ProcessedSectors()
	fmt.Println("Побежали сектора")
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
	for _, item := range Locations {
		fmt.Println(item)
	}
}
