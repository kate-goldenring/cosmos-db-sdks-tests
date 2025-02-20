# Go CosmosDB Test

This expects that you've created a CosmosDB container with partition key `/store_id`. You should have an entry in the container with id `bar` set. Specify the partition key string for that entry in an env var.

## Running

Export the required env vars:

```sh
export COSMOS_ACCOUNT=""
export COSMOS_DATABASE=""
export COSMOS_CONTAINER=""
export COSMOS_AUTH_KEY=""
export COSMOS_PARTITION_KEY_STRING=""
```

Run

```sh
$ go run query.go
[READ] Item ID:  bar
2025/02/19 16:49:16 Function took 534.506375ms
[QUERY] Item ID:  bar
2025/02/19 16:49:16 Function took 92.678375ms
```