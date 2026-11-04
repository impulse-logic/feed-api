package queue

import (
	"fmt"
	"log"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"

	"feed-api/internal/config"
)

// NewClient builds the ONE Service Bus client shared by every queue (central auth).
//
//	SERVICE_BUS_CONNECTION_STRING set => SAS key auth (the SDK creates and renews the tokens)
//	only SERVICE_BUS_NAMESPACE set     => Azure AD auth (az login locally, managed identity on Azure)
//	neither set                        => nil, nil (the app runs; queue routes return 503)
func NewClient(cfg *config.Config) (*azservicebus.Client, error) {
	if cfg.ServiceBusConnectionString != "" {
		client, err := azservicebus.NewClientFromConnectionString(cfg.ServiceBusConnectionString, nil)
		if err != nil {
			// The SDK error can contain the key, so only a generic message is returned.
			return nil, fmt.Errorf("invalid SERVICE_BUS_CONNECTION_STRING")
		}
		log.Println("Service Bus: using connection string (SAS key)")
		return client, nil
	}

	if cfg.ServiceBusNamespace != "" {
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("azure credential: %w", err)
		}
		namespace := strings.TrimSuffix(strings.TrimPrefix(cfg.ServiceBusNamespace, "sb://"), "/")
		client, err := azservicebus.NewClient(namespace, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("service bus client: %w", err)
		}
		log.Printf("Service Bus: using Azure AD credential for %s", namespace)
		return client, nil
	}

	log.Println("Service Bus: not configured (SERVICE_BUS_CONNECTION_STRING / SERVICE_BUS_NAMESPACE are empty)")
	return nil, nil
}

// NewAdminClient builds the management client (same login as NewClient). It is only used to read
// a queue's message count before a run, so a run can skip the end-of-queue wait. That call needs the
// Manage right; without it the consumers fall back to QUEUE_EMPTY_WAIT_MS. nil when not configured.
func NewAdminClient(cfg *config.Config) *admin.Client {
	if cfg.ServiceBusConnectionString != "" {
		client, err := admin.NewClientFromConnectionString(cfg.ServiceBusConnectionString, nil)
		if err != nil {
			return nil
		}
		return client
	}
	if cfg.ServiceBusNamespace != "" {
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil
		}
		namespace := strings.TrimSuffix(strings.TrimPrefix(cfg.ServiceBusNamespace, "sb://"), "/")
		client, err := admin.NewClient(namespace, credential, nil)
		if err != nil {
			return nil
		}
		return client
	}
	return nil
}
