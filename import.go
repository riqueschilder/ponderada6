package importables

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"ponderada6/kafka"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func randFloats(min, max float64) float64 {
	res := min + rand.Float64()*(max-min)
	return res
}

func Publish(client MQTT.Client, repTime time.Duration) {

	for i := 0; i <= len(Topics)-1; i++ {
		topicStringf := fmt.Sprintf("sensor/%s", Topics[i])
		ValuesBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(ValuesBytes, math.Float64bits(Values[Topics[i]]))
		token := client.Publish(topicStringf, 0, false, ValuesBytes)
		token.Wait()
		if token.Error() != nil {
			fmt.Printf("Failed to publish to topic: %s", Topics[i])
			panic(token.Error())
		}

	}
	time.Sleep(repTime * time.Second)
}

func Subscribe(topic string, client MQTT.Client, handler MQTT.MessageHandler) {
	token := client.Subscribe(topic, 1, handler)
	token.Wait()
	if token.Error() != nil {
		fmt.Printf("Failed to subscribe to topic: %v", token.Error())
		panic(token.Error())
	}
	fmt.Printf("\nSubscribed to topic: %s\n", topic)
}

var ConnectHandler MQTT.OnConnectHandler = func(client MQTT.Client) {
	fmt.Println("Connected")
}

var ConnectLostHandler MQTT.ConnectionLostHandler = func(client MQTT.Client, err error) {
	fmt.Printf("Connection lost: %v", err)
}

var MessageHandler MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("Recebido: %f do tópico: %s\n", math.Float64frombits(binary.LittleEndian.Uint64(msg.Payload())), msg.Topic())
	kafka.KafkaMessage(math.Float64frombits(binary.LittleEndian.Uint64(msg.Payload())))
}

var Topics = [3]string{"RED", "OX", "NH3"}

var MapValues = [3]float64{randFloats(1.0, 1000.0), randFloats(0.05, 10.0), randFloats(1.0, 300.0)}

var Values = map[string]float64{
	"RED": MapValues[0],
	"OX":  MapValues[1],
	"NH3": MapValues[2],
}
