package routes

import (
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"feed-api/internal/config"
	"feed-api/internal/handler"
	"feed-api/internal/queue"
	"feed-api/internal/repository"
)

// SetupRouter wires repositories and handlers, then registers every route group.
// sbClient is nil when Service Bus is not configured.
func SetupRouter(db *gorm.DB, cfg *config.Config, sbClient *azservicebus.Client) *gin.Engine {
	r := gin.Default()

	// Vendors
	vendorRepo := repository.NewVendorRepository(db)
	vendorHandler := handler.NewVendorHandler(vendorRepo)
	RegisterVendorRoutes(r.Group("/vendors"), vendorHandler)

	// Product hierarchies
	productHierarchyRepo := repository.NewProductHierarchyRepository(db)
	productHierarchyHandler := handler.NewProductHierarchyHandler(productHierarchyRepo)
	RegisterProductHierarchyRoutes(r.Group("/product-hierarchies"), productHierarchyHandler)

	// Products
	productRepo := repository.NewProductRepository(db)
	productHandler := handler.NewProductHandler(productRepo)
	RegisterProductRoutes(r.Group("/products"), productHandler)

	// Product UPCs
	productUpcRepo := repository.NewProductUpcRepository(db)
	productUpcHandler := handler.NewProductUpcHandler(productUpcRepo)
	RegisterProductUpcRoutes(r.Group("/product-upcs"), productUpcHandler)

	// BOH (back-of-house stock counts)
	bohRepo := repository.NewBohRepository(db)
	bohHandler := handler.NewBohHandler(bohRepo)
	RegisterBohRoutes(r.Group("/boh"), bohHandler)

	// Deliveries
	deliveryRepo := repository.NewDeliveryRepository(db)
	deliveryHandler := handler.NewDeliveryHandler(deliveryRepo)
	RegisterDeliveryRoutes(r.Group("/deliveries"), deliveryHandler)

	// Stocktake
	stockTakeRepo := repository.NewStockTakeRepository(db)
	stockTakeHandler := handler.NewStockTakeHandler(stockTakeRepo)
	RegisterStockTakeRoutes(r.Group("/stocktake"), stockTakeHandler)

	// Product variants
	productVariantRepo := repository.NewProductVariantRepository(db)
	productVariantHandler := handler.NewProductVariantHandler(productVariantRepo)
	RegisterProductVariantRoutes(r.Group("/product-variants"), productVariantHandler)

	// POS (sales + replenishment picklists)
	posRepo := repository.NewPosRepository(db)
	posHandler := handler.NewPosHandler(posRepo)
	RegisterPosRoutes(r.Group("/pos"), posHandler)

	// Azure Service Bus queues (admin only). Each processable queue = one consumer + its processor.
	consumers := map[string]*queue.Consumer{}
	if sbClient != nil {
		consumers["vendor"] = queue.NewConsumer(sbClient, "vendor", queue.NewVendorProcessor(vendorRepo), cfg.QueueWorkers, cfg.QueueBatchSize)
	}
	queueHandler := handler.NewQueueHandler(sbClient, cfg.ServiceBusQueues, consumers)
	// No header needed. To require X-Admin-Token again: r.Group("/queue", handler.RequireAdminToken(cfg.QueueAdminToken))
	RegisterQueueRoutes(r.Group("/queue"), queueHandler)

	return r
}
