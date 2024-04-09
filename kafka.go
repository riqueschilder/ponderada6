package kafka

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func KafkaMessage(message float64) float64 {
	// Mensagem de float para bytes
	byteMessage := make([]byte, 8) // Assuming float64 size is 8 bytes
	binary.LittleEndian.PutUint64(byteMessage, math.Float64bits(message))

	// Configurações do produtor
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"client.id":         "go-producer",
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// Enviar mensagem
	topic := "test_topic"
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          (byteMessage),
	}, nil)

	// Aguardar a entrega de todas as mensagens
	producer.Flush(15 * 1000)

	// Configurações do consumidor
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// Assinar tópico
	consumer.SubscribeTopics([]string{topic}, nil)

	// Consumir mensagens
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Kafka received message: %f on topic %s\n", math.Float64frombits(binary.LittleEndian.Uint64(msg.Value)), *msg.TopicPartition.Topic)
			// addToDb(msg, *msg.TopicPartition.Topic)
			return math.Float64frombits(binary.LittleEndian.Uint64(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}
	return 0
}

func addToDb(msg *kafka.Message, table string) {
	db, _ := sql.Open("sqlite3", "../database/sensor_data.db")
	defer db.Close() // Defer Closing the database

	tableParts := strings.Split(table, "/")

	// Criando a tabla
	sqlStmt := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s
	(id INTEGER PRIMARY KEY, sensorValue FLOAT, time DATETIME)
	`, tableParts[1])
	// Preparando o sql statement de forma segura
	command, err := db.Prepare(sqlStmt)
	if err != nil {
		log.Fatal(err.Error())
	}
	// Executando o comando sql
	command.Exec()

	// Criando uma função para inserir usuários
	insertData := func(db *sql.DB, data float64) {
		stmt := fmt.Sprintf(`INSERT INTO %s(sensorValue, time) VALUES (?, ?)`, tableParts[1])
		statement, err := db.Prepare(stmt)
		if err != nil {
			log.Fatalln(err.Error())
		}
		_, err = statement.Exec(data, time.Now())
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	insertData(db, math.Float64frombits(binary.LittleEndian.Uint64(msg.Value)))
}
