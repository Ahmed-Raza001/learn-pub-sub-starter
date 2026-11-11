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

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		"game_logs",
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueDurable,
		func(gameLog routing.GameLog) pubsub.AckType {
			defer fmt.Print("> ")

			err := gamelogic.WriteLog(gameLog)
			if err != nil {
				fmt.Println("Failed to write game log:", err)
				return pubsub.NackRequeue
			}

			fmt.Println("Game log saved:", gameLog.Message)
			return pubsub.Ack
		},
	)

	if err != nil {
		fmt.Println("Failed to subscribe to game logs:", err)
		return
	}

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
