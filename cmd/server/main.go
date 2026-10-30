package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	// Connect to RabbitMQ
	connString := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connString)
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ!")

	// Declare and bind durable game_logs queue
	gameLogCh, _, err := pubsub.DeclareAndBind(
		conn,
		"peril_topic",
		"game_logs",
		"game_logs.*",
		pubsub.SimpleQueueDurable,
	)
	if err != nil {
		fmt.Println("Failed to declare game_logs queue:", err)
		return
	}
	defer gameLogCh.Close()

	// Create channel
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open channel:", err)
		return
	}
	defer ch.Close()

	// Print server commands
	gamelogic.PrintServerHelp()

	// Start infinite loop
	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {

		case "pause":
			fmt.Println("Sending pause message...")

			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)

			if err != nil {
				fmt.Println("Failed to publish pause message:", err)
			}

		case "resume":
			fmt.Println("Sending resume message...")

			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)

			if err != nil {
				fmt.Println("Failed to publish resume message:", err)
			}

		case "quit":
			fmt.Println("Exiting server...")
			return

		default:
			fmt.Println("I don't understand the command.")
		}
	}

}
