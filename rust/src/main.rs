mod store;

#[tokio::main]
async fn main() {
    // Get values from env vars
    let account = std::env::var("COSMOS_ACCOUNT").expect("AZURE_COSMOS_ACCOUNT is not set");
    let key = std::env::var("COSMOS_AUTH_KEY").expect("AZURE_COSMOS_KEY is not set");
    let database = std::env::var("COSMOS_DATABASE").expect("COSMOS_DATABASE is not set");
    let container = std::env::var("COSMOS_CONTAINER").expect("COSMOS_CONTAINER is not set");
    let config = store::AzureCosmosKeyValueRuntimeConfig {
        account,
        key,
        database,
        container,
    };

    let kv = store::KeyValueAzureCosmos::new(
        Some("cosmos".to_string()),
        Some("default".to_string()),
        config,
    )
    .unwrap();

    // set key
    let start = std::time::Instant::now();
    kv.set("key", b"value").await.expect("set key failed");
    let duration = start.elapsed();
    println!("Set execution time: {:?}", duration);
    // get key
    let start = std::time::Instant::now();
    let value = kv.get("key").await.expect("get key failed");
    let duration = start.elapsed();
    println!("Get execution time: {:?}", duration);
    match value {
        Some(value) => println!("Value is: {:?}", String::from_utf8(value).unwrap()),
        None => println!("key not found"),
    }
}
