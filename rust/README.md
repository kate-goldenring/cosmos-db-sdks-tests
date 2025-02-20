# Rust CosmosDB SDK

This code is sampled from the `spin-key-value-azure"` crate in order to test its performance.

## Running
Export the required env vars:

```sh
export COSMOS_ACCOUNT=""
export COSMOS_DATABASE=""
export COSMOS_CONTAINER=""
export COSMOS_AUTH_KEY=""
```

Run

```sh
$ cargo run 
Set execution time: 451.417709ms
Get execution time: 362.852417ms
Value is: "value"
```