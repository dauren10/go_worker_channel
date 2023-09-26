package main

import (
	"fmt"
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

func ProcessedSectors(fch chan map[string]int) {
	FinishedLocations := make(map[string]int)

	for i := 0; i < 10; i++ {
		msg := i
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
		time.Sleep(1 * time.Second)
		fmt.Println("Проверка сектора", msg)
	}

	fch <- FinishedLocations
	close(fch)
}

func main() {
	fch := make(chan map[string]int)
	go ProcessedSectors(fch)
	receivedMap := <-fch
	fmt.Println("Отправка секторов в cond response")
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

	errorLocation := make(map[string]int)

	for key, value := range Locations {
		if _, ok := receivedMap[key]; ok {

		} else {
			errorLocation[key] = value
		}
	}

	for _, value := range errorLocation {
		fmt.Println("Ошибочный сектор", value)
	}
}
