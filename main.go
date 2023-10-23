package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func worker(id int) {
	// Подключение к RabbitMQ серверу
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

	// Объявление очереди
	q, err := ch.QueueDeclare(
		"processed_sectors", // Название очереди
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	// Получение сообщений из очереди
	msgs, err := ch.Consume(
		q.Name, // Название очереди
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	fmt.Printf("Worker %d started\n", id)

	// Чтение сообщений из очереди
	for msg := range msgs {
		msg.Ack(false)
		fmt.Printf("Worker %d received message: %s\n", id, msg.Body)
		time.Sleep(time.Second * 1)
	}
}

func main() {
	// Запуск нескольких воркеров
	for i := 1; i <= 3; i++ {
		go worker(i)
	}

	// Ждем, чтобы воркеры имели время обработать сообщения
	time.Sleep(5 * time.Second)

	// Чтобы программа не завершилась слишком рано
	fmt.Println("Workers running...")
	<-make(chan struct{})
}
