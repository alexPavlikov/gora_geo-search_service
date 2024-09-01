package config

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Timeout              time.Duration `mapstructure:"timeout"`
	LogLevel             int           `mapstructure:"log_level"`
	Topic                string        `mapstructure:"topic"`
	PostgresUser         string        `mapstructure:"pg_user"`
	PostgresPassword     string        `mapstructure:"pg_password"`
	PostgresDatabaseName string        `mapstructure:"pg_dbname"`
	Server               Server        `mapstructure:"server"`
	KafkaAddress         Server        `mapstructure:"kafka"`
	PostgresAddress      Server        `mapstructure:"pg_address"`
}

type Server struct {
	Path string `mapstructure:"path"`
	Port int    `mapstructure:"port"`
}

func (s *Server) ToString() string {
	return fmt.Sprintf("%s:%d", s.Path, s.Port)
}

func Load() (*Config, error) {

	var cfg Config

	file, path, err := getConfigPath()
	if err != nil {
		return nil, fmt.Errorf("load config err: %w", err)
	}

	if file == "" || path == "" {
		return nil, fmt.Errorf("load config err: %w", errors.New("file or path to file is empty"))
	}

	cfg, err = initViper(file, path, cfg)
	if err != nil {
		return nil, fmt.Errorf("load config err: %w", err)
	}

	var level slog.Level = slog.Level(cfg.LogLevel)
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	slog.SetDefault(log)

	return &cfg, nil
}

func getConfigPath() (file string, path string, err error) {

	flag.StringVar(&file, "file", "", "config file")
	flag.StringVar(&path, "path", "", "path to config file")
	flag.Parse()

	if path == "" {
		path = os.Getenv("GEO-SEARCH-CONFIG-PATH")
	}

	if file == "" {
		file = os.Getenv("GEO-SEARCH-CONFIG-FILE")
	}

	file = "config"
	path = "./config/"

	return file, path, nil
}

func initViper(file string, path string, cfg Config) (Config, error) {
	viper.SetConfigName(file)
	viper.AddConfigPath(path)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("failed viper read in cofig: %w", err)
	}

	if err := viper.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("failed viper unmarshal config: %w", err)
	}

	return cfg, nil
}
