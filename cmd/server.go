package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/alexPavlikov/gora_geo-search_service/internal/config"
	"github.com/alexPavlikov/gora_geo-search_service/internal/models"
	"github.com/alexPavlikov/gora_geo-search_service/internal/storage"
	"golang.org/x/exp/rand"
)

func Run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed run config load: %w", err)
	}

	slog.Info("config read", "cfg", cfg.Server.ToString())

	//-------------------------------пример-------------------------------
	producer, err := storage.GetProducer(cfg.KafkaAddress.ToString())
	if err != nil {
		return fmt.Errorf("failed get producer: %w", err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			var crd = models.Cord{
				DriverID:  rand.Intn(100000),
				Latitude:  float32(rand.Intn(100000)),
				Longitude: float32(rand.Intn(100000)),
			}

			if err := storage.SendMessage(context.TODO(), crd, *cfg, producer); err != nil {
				slog.Error("failed send message to kafka", "error", err)
				return
			}
		}
	}()
	defer wg.Wait()

	//--------------------------------------------------------------

	consumer, err := storage.GetConsumer(cfg.KafkaAddress.ToString())
	if err != nil {
		return fmt.Errorf("failed get consumer: %w", err)
	}

	conn, err := storage.Connect(context.TODO(), cfg)
	if err != nil {
		slog.Info("failed connect to database", "error", err)
	}

	repo := storage.New(conn)

	err = repo.ReadMessageFromKafka(consumer, cfg.Topic)
	if err != nil {
		return fmt.Errorf("failed read message from kafka: %w", err)
	}

	return nil
}
