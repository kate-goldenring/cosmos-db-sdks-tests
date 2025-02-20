package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

type Document struct {
	ID          string `json:"id"`
	Value       []byte `json:"value"`
	StoreID     string `json:"store_id"`
	Rid         string `json:"_rid"`
	Self        string `json:"_self"`
	Etag        string `json:"_etag"`
	Attachments string `json:"_attachments"`
	Timestamp   int64  `json:"_ts"`
}

func main() {
	// Cosmos DB connection details
	account := os.Getenv("COSMOS_ACCOUNT")
	key := os.Getenv("COSMOS_AUTH_KEY")
	databaseName := os.Getenv("COSMOS_DATABASE")
	containerName := os.Getenv("COSMOS_CONTAINER")
	endpoint := fmt.Sprintf("https://%s.documents.azure.com:443/", account)
	partitionKeyString, bool := os.LookupEnv("COSMOS_PARTITION_KEY_STRING")
	if !bool {
		partitionKeyString = "cosmos/default"
	}

	// Create a Cosmos client
	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		log.Fatalf("Failed to create Cosmos DB credential: %v", err)
	}
	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		log.Fatalf("Failed to create Cosmos DB client: %v", err)
	}

	// Create a container client
	containerClient, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		log.Fatalf("Failed to get container client: %v", err)
	}
	pk := azcosmos.NewPartitionKeyString(partitionKeyString)
	// Assuming a bar item exists
	timeFuncExecution(func() { readItem(containerClient, "bar", pk) })
	timeFuncExecution(func() { queryItem(containerClient, "bar", pk) })
}

func timeFuncExecution(f func()) {
	start := time.Now()
	f()
	elapsed := time.Since(start)
	log.Printf("Function took %s", elapsed)
}

func readItem(containerClient *azcosmos.ContainerClient, id string, pk azcosmos.PartitionKey) {

	context := context.TODO()

	response, err := containerClient.ReadItem(context, pk, id, nil)
	if err != nil {
		fmt.Println("Failed to read item")
		return
	}
	if response.RawResponse.StatusCode == 200 {
		read_item := Document{}

		err := json.Unmarshal(response.Value, &read_item)
		if err != nil {
			fmt.Println("Failed to unmarshal item")
			return
		}
		println("[READ] Item ID: ", read_item.ID)
	}
}

func queryItem(containerClient *azcosmos.ContainerClient, id string, pk azcosmos.PartitionKey) {
	// Query by id
	query := "SELECT * FROM c WHERE c.id = @id AND c.store_id = @store_id"
	queryParams := []azcosmos.QueryParameter{{Name: "@id", Value: id}, {Name: "@store_id", Value: "cosmos/default"}}
	queryOptions := azcosmos.QueryOptions{
		QueryParameters: queryParams,
	}

	pager := containerClient.NewQueryItemsPager(query, pk, &queryOptions)

	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("Failed to query items: %v", err)
		}
		for _, item := range resp.Items {
			read_item := Document{}
			err := json.Unmarshal(item, &read_item)
			if err != nil {
				fmt.Println("Failed to unmarshal item")
				return
			}
			println("[QUERY] Item ID: ", read_item.ID)
		}
	}
}
