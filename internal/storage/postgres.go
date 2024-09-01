package storage

import (
	"context"
	"fmt"

	"github.com/alexPavlikov/gora_geo-search_service/internal/config"
	"github.com/alexPavlikov/gora_geo-search_service/internal/models"
	"github.com/jackc/pgx/v4/pgxpool"
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
	conn, err = pgxpool.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed connect to database: %w", err)
	}
	return conn, nil
}

func (r *Repository) InsertBatchCordToGIS(ctx context.Context, cords map[int]models.Cord) error {
	for _, cord := range cords {
		query := `
		INSERT INTO public."cords" (driver_id, latitude, longitude) VALUES ($1, $2, $3) 
		ON CONFLICT (driver_id) DO UPDATE SET driver_id = $4, latitude = $5, longitude = $6 
		RETURNING id
		`

		row := r.DB.QueryRow(ctx, query, cord.DriverID, cord.Latitude, cord.Longitude, cord.DriverID, cord.Latitude, cord.Longitude)

		var id int

		if err := row.Scan(&id); err != nil {
			return fmt.Errorf("insert cord error: %w", err)
		}

	}
	return nil
}
