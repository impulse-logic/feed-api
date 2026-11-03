package main

import (
	"context"
	"log"

	"github.com/gin-gonic/gin"

	"feed-api/internal/config"
	"feed-api/internal/database"
	"feed-api/internal/queue"
	"feed-api/internal/routes"
)

func main() {
	// 1. Load Config & Connect DB
	cfg := config.LoadConfig()
	// Gin reads GIN_MODE before .env is loaded, so apply it explicitly
	gin.SetMode(cfg.GinMode)

	db := database.ConnectDB(cfg)

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("Failed to get underlying sql.DB: %v", err)
	}
	defer sqlDB.Close()

	// 2. Create/update tables (opt-in: only for databases this app owns)
	if cfg.AutoMigrate {
		database.Migrate(db)
	}

	// 3. Azure Service Bus client (one for all queues; nil when not configured)
	sbClient, err := queue.NewClient(cfg)
	if err != nil {
		log.Fatalf("Service Bus setup failed: %v", err)
	}
	if sbClient != nil {
		defer sbClient.Close(context.Background())
	}

	// 4. Setup Router (all route files are registered in internal/routes)
	r := routes.SetupRouter(db, cfg, sbClient)

	// 5. Start Server
	log.Printf("Server starting on port %s...", cfg.ServerPort)
	if err := r.Run(":" + cfg.ServerPort); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
