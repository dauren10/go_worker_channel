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

func ProcessedSectors(fch chan SectorResponse) {
	for i := 0; i < 10; i++ {
		msg := i
		fmt.Println("Проверка сектора", msg)
		response := SectorResponse{
			TaskID:     msg,
			RoundNo:    msg,
			SectorID:   msg,
			CellID:     msg,
			LAC:        msg,
			Service:    "service",
			Timestamp:  time.Now(),
			ReturnCode: msg,
			Result:     "result",
			Reason:     "reason",
		}
		fch <- response
		time.Sleep(1 * time.Second)

	}

	close(fch)
}

func main() {
	fch := make(chan SectorResponse)
	go ProcessedSectors(fch)
	receivedSlice := []SectorResponse{}
	fmt.Println("Отправка секторов в cond response")

	for {
		select {
		case item, ok := <-fch:
			if !ok {
				// Канал закрыт, выходим из цикла
				return
			}
			receivedSlice = append(receivedSlice, item)
			fmt.Printf("%s,%+v\n", "отправка", item.SectorID)
		}
	}
}
