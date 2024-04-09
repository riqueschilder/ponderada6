package main

import (
	"fmt"
	"os"

	importables "ponderada6/importables"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	godotenv "github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
)

func main() {

	username := os.Getenv("USERNAME")
	password := os.Getenv("PASSWORD")
	if username == "" || password == "" {
		// GitHub Secrets not found, try loading from .env file
		err := godotenv.Load("../.env")
		if err != nil {
			fmt.Println("Error loading .env file")
			return
		}

		username = os.Getenv("HIVE_USER")
		password = os.Getenv("HIVE_PSWD")
	}
	var broker = os.Getenv("BROKER_ADDR")
	var port = 8883
	opts := MQTT.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tls://%s:%d", broker, port))
	opts.SetClientID("Ponderada6")
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetDefaultPublishHandler(importables.MessageHandler)
	opts.OnConnect = importables.ConnectHandler
	opts.OnConnectionLost = importables.ConnectLostHandler

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	importables.Subscribe("sensor/+", client, importables.MessageHandler)
	importables.Publish(client, 1)

	client.Disconnect(300000)
}
