package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v4/pgxpool"
)

func main() {

	var cfg = Config{
		Timeout:  2 * time.Second,
		LogLevel: "",
		Server: Server{
			Path: "localhost",
			Port: 8080,
		},
		KafkaAddres: Server{
			Path: "localhost",
			Port: 9092,
		},
		Topic: "driver",
		PostgresAddress: Server{
			Path: "localhost",
			Port: 5432,
		},
		PostgresUser:         "postgres",
		PostgresPassword:     "AlexPAV2307",
		PostgresDatabaseName: "cord",
	}

	slog.Info("start", "config", cfg)

	producer, err := GetProducer(cfg.ToString())
	if err != nil {
		slog.Error("failed get producer", "error", err)
		return
	}

	var crd = Cord{
		DriverID:  1,
		Latitude:  2,
		Longitude: 3,
	}

	if err := SendMessage(context.TODO(), crd, cfg, producer); err != nil {
		slog.Error("failed send message to kafka", "error", err)
		return
	}

	consumer, stop, err := GetConsumer(cfg.ToString())
	if err != nil {
		slog.Error("failed get consumer", "error", err)
		return
	}

	defer stop()

	msg, err := ReadMessageFromKafka(consumer, cfg.Topic)
	if err != nil {
		slog.Error("failed read message from kafka", "error", err)
		return
	}

	slog.Info("Message", "msg", msg)

	cord := Cord{
		DriverID:  1,
		Latitude:  2,
		Longitude: 3,
	}

	slog.Info("cords from kafka", "data", cord)

	conn, err := Connect(context.TODO(), &cfg)
	if err != nil {
		slog.Info("failed connect to database", "error", err)
	}

	repo := New(context.TODO(), conn)

	if err := repo.InsertCordToGIS(context.TODO(), cord); err != nil {
		slog.Info("failed insert cord to postgres", "error", err)
		return
	}

}

type Config struct {
	Timeout              time.Duration `mapstructure:"timeout"`
	LogLevel             string        `mapstructure:"loglevel"`
	Server               Server        `mapstructure:"server"`
	KafkaAddres          Server        `mapstructure:"kafka"`
	Topic                string        `mapstructure:"topic"`
	PostgresAddress      Server        `mapstructure:"postgres"`
	PostgresUser         string        `mapstructure:"postgres_user"`
	PostgresPassword     string        `mapstructure:"postgres_password"`
	PostgresDatabaseName string        `mapstructure:"postgres_database"`
}

func (c *Config) ToString() string {
	return c.KafkaAddres.Path + ":" + fmt.Sprint(c.KafkaAddres.Port)
}

type Server struct {
	Path string `mapstructure:"SERVER_PATH"`
	Port int    `mapstructure:"SERVER_PORT"`
}

type Cord struct {
	DriverID  int     `json:"driver_id"`
	Latitude  float32 `json:"latitude"`
	Longitude float32 `json:"longitude"`
}

// kafka
func GetConsumer(address ...string) (sarama.Consumer, func() error, error) {
	consumer, err := sarama.NewConsumer(address, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed get new consumer: %w", err)
	}

	return consumer, consumer.Close, nil
}

func ReadMessageFromKafka(consumer sarama.Consumer, topic string) (cord Cord, err error) {
	partition, err := consumer.Partitions(topic)
	if err != nil {
		return Cord{}, fmt.Errorf("failed to get partition: %w", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition[0], sarama.OffsetNewest)
	if err != nil {
		return Cord{}, fmt.Errorf("failed to get ConsumePartition: %w", err)
	}

	defer partitionConsumer.Close()

	msg := <-partitionConsumer.Messages()

	if err := json.Unmarshal(msg.Value, &cord); err != nil {
		return Cord{}, fmt.Errorf("failed unmarshal result from kafka: %w", err)
	}

	return cord, nil
}

func GetProducer(address ...string) (producer sarama.SyncProducer, err error) {
	producer, err = sarama.NewSyncProducer(address, nil) //....
	if err != nil {
		return nil, fmt.Errorf("error creating producer: %w", err)
	}

	return producer, nil
}

func SendMessage(ctx context.Context, cord Cord, cfg Config, producer sarama.SyncProducer) error {

	cordJSON, err := json.Marshal(cord)
	if err != nil {
		return fmt.Errorf("failed to marshal cord: %w", err)
	}

	var msg = sarama.ProducerMessage{
		Topic:     cfg.Topic,
		Key:       sarama.StringEncoder(fmt.Sprint(cord.DriverID)),
		Value:     sarama.ByteEncoder(cordJSON),
		Timestamp: time.Now(),
	}

	slog.Info("send message to kafka", "message key", cord.DriverID)

	if _, _, err := producer.SendMessage(&msg); err != nil {
		return err
	}

	return nil
}

// postgres

type Repository struct {
	ctx context.Context
	DB  *pgxpool.Pool
}

func New(ctx context.Context, DB *pgxpool.Pool) *Repository {
	return &Repository{
		ctx: ctx,
		DB:  DB,
	}
}

func Connect(ctx context.Context, cfg *Config) (conn *pgxpool.Pool, err error) {
	databaseURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.PostgresUser, cfg.PostgresPassword, cfg.PostgresAddress.Path, cfg.PostgresAddress.Port, cfg.PostgresDatabaseName)
	conn, err = pgxpool.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed connect to database: %w", err)
	}
	return conn, nil
}

func (r *Repository) InsertCordToGIS(ctx context.Context, cord Cord) error {
	query := `
	INSERT INTO public."cords" (driver_id, latitude, longitude) VALUES ($1, $2, $3) RETURNING id
	`

	row := r.DB.QueryRow(ctx, query, cord.DriverID, cord.Latitude, cord.Longitude)

	var id int

	if err := row.Scan(&id); err != nil {
		return fmt.Errorf("insert cord error: %w", err)
	}

	return nil
}
