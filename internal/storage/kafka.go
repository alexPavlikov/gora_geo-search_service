package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/alexPavlikov/gora_geo-search_service/internal/config"
	"github.com/alexPavlikov/gora_geo-search_service/internal/models"
)

const (
	LEN_BATCH          = 15
	TICKER_SECOND_TIME = 5
)

// kafka
func GetConsumer(address ...string) (sarama.Consumer, error) {
	consumer, err := sarama.NewConsumer(address, nil)
	if err != nil {
		return nil, fmt.Errorf("failed get new consumer: %w", err)
	}

	return consumer, nil
}

func (r *Repository) ReadMessageFromKafka(consumer sarama.Consumer, topic string) error {
	partition, err := consumer.Partitions(topic)
	if err != nil {
		return fmt.Errorf("failed to get partition: %w", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition[0], sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("failed to get ConsumePartition: %w", err)
	}

	defer partitionConsumer.Close()

	ticker := time.NewTicker(TICKER_SECOND_TIME * time.Second)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		var cords = make(map[int]models.Cord)
		var cord models.Cord

		for {
			select {
			case msg := <-partitionConsumer.Messages():
				if err := json.Unmarshal(msg.Value, &cord); err != nil {
					slog.Error("failed unmarshal result from kafka", "error", err)
					return
				}

				cords[cord.DriverID] = cord

				if len(cords) == LEN_BATCH {
					if err := r.InsertBatchCordToGIS(context.TODO(), cords); err != nil {
						slog.Error("failed insert cords to postgres", "error", err)
						return
					}
				}

			case <-ticker.C:
				if err := r.InsertBatchCordToGIS(context.TODO(), cords); err != nil {
					slog.Error("failed insert cords to postgres", "error", err)
					return
				}
			}
		}
	}()

	wg.Wait()
	return nil
}

//-------------------------------пример-------------------------------

func GetProducer(address ...string) (producer sarama.SyncProducer, err error) {
	producer, err = sarama.NewSyncProducer(address, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating producer: %w", err)
	}

	return producer, nil
}

func SendMessage(ctx context.Context, cord models.Cord, cfg config.Config, producer sarama.SyncProducer) error {

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

	if _, _, err := producer.SendMessage(&msg); err != nil {
		return err
	}

	return nil
}
