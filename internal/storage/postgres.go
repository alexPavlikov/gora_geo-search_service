package storage

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/alexPavlikov/gora_geo-search_service/internal/config"
	"github.com/alexPavlikov/gora_geo-search_service/internal/models"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	DB *pgxpool.Pool
}

func New(DB *pgxpool.Pool) *Repository {
	return &Repository{
		DB: DB,
	}
}

func Connect(ctx context.Context, cfg *config.Config) (conn *pgxpool.Pool, err error) {
	databaseURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.PostgresUser, cfg.PostgresPassword, cfg.PostgresAddress.Path, cfg.PostgresAddress.Port, cfg.PostgresDatabaseName)
	conn, err = pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed connect to database: %w", err)
	}
	return conn, nil
}

func (r *Repository) InsertBatchCordToGIS(ctx context.Context, cords map[int]models.Cord) error {

	query := `
		INSERT INTO public."cords" (driver_id, latitude, longitude) VALUES (@driver_id, @latitude, @longitude)
		ON CONFLICT (driver_id) DO UPDATE SET driver_id = @driver_id, latitude = @latitude, longitude =  @longitude
		RETURNING id
	`

	batch := &pgx.Batch{}

	for _, v := range cords {
		args := pgx.NamedArgs{
			"driver_id": v.DriverID,
			"latitude":  v.Latitude,
			"longitude": v.Longitude,
		}
		batch.Queue(query, args)
	}

	result := r.DB.SendBatch(ctx, batch)
	defer result.Close()

	for _, cord := range cords {
		_, err := result.Exec()
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
				log.Printf("driver %d already exists", cord.DriverID)
				continue
			}

			return fmt.Errorf("unable to insert row: %w", err)
		}
	}

	return result.Close()
}
