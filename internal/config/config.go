package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	DBHost      string
	DBPort      string
	DBUser      string
	DBPassword  string
	DBName      string
	DBSSLMode   string
	AutoMigrate bool
	ServerPort  string
	GinMode     string

	// Azure Service Bus (see docs/SERVICE_BUS.md)
	ServiceBusConnectionString string
	ServiceBusNamespace        string
	ServiceBusQueues           []string
	QueueAdminToken            string
	QueueWorkers               int
	QueueBatchSize             int
}

func LoadConfig() *Config {
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found, using system environment variables")
	}

	return &Config{
		DBHost:      getEnv("DB_HOST", "localhost"),
		DBPort:      getEnv("DB_PORT", "5432"),
		DBUser:      getEnv("DB_USER", "postgres"),
		DBPassword:  getEnv("DB_PASSWORD", ""),
		DBName:      getEnv("DB_NAME", "postgres"),
		DBSSLMode:   getEnv("DB_SSLMODE", "disable"),
		AutoMigrate: getEnv("DB_AUTO_MIGRATE", "false") == "true",
		ServerPort:  getEnv("SERVER_PORT", "8080"),
		GinMode:     getEnv("GIN_MODE", "debug"),

		ServiceBusConnectionString: getEnv("SERVICE_BUS_CONNECTION_STRING", ""),
		ServiceBusNamespace:        getEnv("SERVICE_BUS_NAMESPACE", ""),
		ServiceBusQueues:           splitList(getEnv("SERVICE_BUS_ENTITY", "vendor")),
		QueueAdminToken:            getEnv("QUEUE_ADMIN_TOKEN", ""),
		QueueWorkers:               getEnvInt("QUEUE_WORKERS", 8, 1, 32),
		QueueBatchSize:             getEnvInt("QUEUE_BATCH_SIZE", 100, 1, 1000),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// splitList turns "vendor, product" into ["vendor", "product"] (empty items are dropped).
func splitList(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

// getEnvInt reads a whole number in [min, max]; anything else falls back to the default.
func getEnvInt(key string, fallback, min, max int) int {
	n, err := strconv.Atoi(getEnv(key, ""))
	if err != nil || n < min || n > max {
		return fallback
	}
	return n
}
