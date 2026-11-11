package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

// Handle pause and resume messages
func handlerPause(
	gs *gamelogic.GameState,
) func(routing.PlayingState) pubsub.AckType {

	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")

		gs.HandlePause(state)

		return pubsub.Ack
	}
}

// Handle army movement messages
func handlerMove(
	gs *gamelogic.GameState,
	ch *amqp.Channel,
	username string,
) func(gamelogic.ArmyMove) pubsub.AckType {

	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		outcome := gs.HandleMove(move)

		fmt.Println("Move detected successfully!")

		switch outcome {

		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack

		case gamelogic.MoveOutcomeMakeWar:

			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+username,
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)

			if err != nil {
				fmt.Println("Failed to publish war message:", err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack

		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard

		default:
			return pubsub.NackDiscard
		}
	}
}

// Publish GameLog message
func publishGameLog(
	ch *amqp.Channel,
	username string,
	gameLog routing.GameLog,
) error {

	routingKey := routing.GameLogSlug + "." + username

	return pubsub.PublishGob(
		ch,
		routing.ExchangePerilTopic,
		routingKey,
		gameLog,
	)
}

// Handle war messages
func handlerWar(
	gs *gamelogic.GameState,
	ch *amqp.Channel,
) func(amqp.Delivery) {

	return func(msg amqp.Delivery) {

		defer fmt.Print("> ")

		var war gamelogic.RecognitionOfWar

		err := json.Unmarshal(msg.Body, &war)
		if err != nil {
			fmt.Println("Error unmarshaling war message:", err)
			msg.Nack(false, false)
			return
		}

		outcome, winner, loser := gs.HandleWar(war)

		var message string

		switch outcome {

		case gamelogic.WarOutcomeNotInvolved:

			msg.Nack(false, true)
			return

		case gamelogic.WarOutcomeNoUnits:

			msg.Nack(false, false)
			return

		case gamelogic.WarOutcomeOpponentWon:

			message = fmt.Sprintf(
				"%s won a war against %s",
				winner,
				loser,
			)

		case gamelogic.WarOutcomeYouWon:

			message = fmt.Sprintf(
				"%s won a war against %s",
				winner,
				loser,
			)

		case gamelogic.WarOutcomeDraw:

			message = fmt.Sprintf(
				"A war between %s and %s resulted in a draw",
				winner,
				loser,
			)

		default:

			fmt.Println("Unknown war outcome:", outcome)
			msg.Nack(false, false)
			return
		}

		// Create GameLog
		gameLog := routing.GameLog{
			CurrentTime: time.Now(),
			Message:     message,
			Username:    war.Attacker.Username,
		}

		// Publish GameLog
		err = publishGameLog(
			ch,
			war.Attacker.Username,
			gameLog,
		)

		if err != nil {
			fmt.Println("Failed to publish game log:", err)
			msg.Nack(false, true)
			return
		}

		fmt.Println("Game log published:", message)

		msg.Ack(false)
	}
}

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

	// Get username
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Failed to get username:", err)
		return
	}

	// Create game state
	gamestate := gamelogic.NewGameState(username)

	// Create RabbitMQ channel
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to open channel:", err)
		return
	}
	defer ch.Close()

	// Subscribe to pause and resume messages
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gamestate),
	)

	if err != nil {
		fmt.Println("Failed to subscribe to pause messages:", err)
		return
	}

	// Subscribe to army movement messages
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+username,
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gamestate, ch, username),
	)

	if err != nil {
		fmt.Println("Failed to subscribe to army moves:", err)
		return
	}

	// Declare durable war queue
	_, err = ch.QueueDeclare(
		"war",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatal("Failed to declare war queue:", err)
	}

	// Bind war queue to topic exchange
	err = ch.QueueBind(
		"war",
		routing.WarRecognitionsPrefix+".#",
		routing.ExchangePerilTopic,
		false,
		nil,
	)

	if err != nil {
		log.Fatal("Failed to bind war queue:", err)
	}

	// Consume war messages
	msgs, err := ch.Consume(
		"war",
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatal("Failed to consume war messages:", err)
	}

	// Handle war messages in background
	go func() {
		for msg := range msgs {
			handlerWar(gamestate, ch)(msg)
		}
	}()

	// REPL loop
	for {

		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {

		case "spawn":

			err := gamestate.CommandSpawn(words)
			if err != nil {
				fmt.Println("Error spawning unit:", err)
				continue
			}

		case "move":

			move, err := gamestate.CommandMove(words)
			if err != nil {
				fmt.Println("Error moving unit:", err)
				continue
			}

			// Publish move to other clients
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+username,
				move,
			)

			if err != nil {
				fmt.Println("Failed to publish move:", err)
				continue
			}

			fmt.Println("Move published successfully!")

		case "status":

			gamestate.CommandStatus()

		case "help":

			gamelogic.PrintClientHelp()

		case "spam":

			if len(words) < 2 {
				fmt.Println("Usage: spam <number>")
				continue
			}

			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Println("Invalid number:", words[1])
				continue
			}

			for i := 0; i < n; i++ {
				message := gamelogic.GetMaliciousLog()

				gameLog := routing.GameLog{
					CurrentTime: time.Now(),
					Message:     message,
					Username:    username,
				}

				routingKey := routing.GameLogSlug + "." + username

				err := pubsub.PublishGob(
					ch,
					routing.ExchangePerilTopic,
					routingKey,
					gameLog,
				)

				if err != nil {
					fmt.Println("Failed to publish game log:", err)
					break
				}
			}

			fmt.Printf("Successfully published %d game logs!\n", n)

		case "quit":

			gamelogic.PrintQuit()
			return

		default:

			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}
