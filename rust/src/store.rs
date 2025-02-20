use anyhow::Result;
use azure_data_cosmos::{
    prelude::{AuthorizationToken, CollectionClient, CosmosClient, Query},
    CosmosEntity,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

pub struct AzureKeyValueStore {
    app_id: Option<String>,
}

impl AzureKeyValueStore {
    /// Creates a new `AzureKeyValueStore`.
    ///
    /// When `app_id` is provided, the store will a partition key of `$app_id/$store_name`,
    /// otherwise the partition key will be `id`.
    pub fn new(app_id: Option<String>) -> Self {
        Self { app_id }
    }
}

pub struct KeyValueAzureCosmos {
    client: CollectionClient,
    /// An optional app id
    ///
    /// If provided, the store will handle multiple stores per container using a
    /// partition key of `/$app_id/$store_name`, otherwise there will be one container
    /// per store, and the partition key will be `/id`.
    app_id: Option<String>,
    /// An optional store id to use as a partition key for all operations.
    ///
    /// If the store id not set, the store will use `/id` as the partition key.
    store_id: Option<String>,
}

/// Runtime configuration for the Azure Cosmos key-value store.
#[derive(Deserialize)]
pub struct AzureCosmosKeyValueRuntimeConfig {
    /// The authorization token for the Azure Cosmos DB account.
    pub key: String,
    /// The Azure Cosmos DB account name.
    pub account: String,
    /// The Azure Cosmos DB database.
    pub database: String,
    /// The Azure Cosmos DB container where data is stored.
    /// The CosmosDB container must be created with the default partition key, /id
    pub container: String,
}

impl KeyValueAzureCosmos {
    pub fn new(
        app_id: Option<String>,
        store_id: Option<String>,
        config: AzureCosmosKeyValueRuntimeConfig,
    ) -> Result<Self> {
        let token = AuthorizationToken::primary_key(config.key).map_err(log_error)?;
        let cosmos_client = CosmosClient::new(config.account, token);
        let database_client = cosmos_client.database_client(config.database);
        let client = database_client.collection_client(config.container);

        Ok(Self {
            client,
            app_id,
            store_id,
        })
    }
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let pair = self.get_pair(key).await?;
        Ok(pair.map(|p| p.value))
    }

    pub async fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let pair = Pair {
            id: key.to_string(),
            value: value.to_vec(),
            store_id: self.store_id.clone(),
        };
        self.client
            .create_document(pair)
            .is_upsert(true)
            .await
            .map_err(log_error)?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        if self.exists(key).await? {
            let document_client = self
                .client
                .document_client(key, &self.store_id)
                .map_err(log_error)?;
            document_client.delete_document().await.map_err(log_error)?;
        }
        Ok(())
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.get_pair(key).await?.is_some())
    }

    pub async fn get_many(&self, keys: Vec<String>) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        let stmt = Query::new(self.get_in_query(keys));
        let query = self
            .client
            .query_documents(stmt)
            .query_cross_partition(true);

        let mut res = Vec::new();
        let mut stream = query.into_stream::<Pair>();
        while let Some(resp) = stream.next().await {
            let resp = resp.map_err(log_error)?;
            res.extend(
                resp.results
                    .into_iter()
                    .map(|(pair, _)| (pair.id, Some(pair.value))),
            );
        }
        Ok(res)
    }

    pub async fn set_many(&self, key_values: Vec<(String, Vec<u8>)>) -> Result<()> {
        for (key, value) in key_values {
            self.set(key.as_ref(), &value).await?
        }
        Ok(())
    }

    pub async fn delete_many(&self, keys: Vec<String>) -> Result<()> {
        for key in keys {
            self.delete(key.as_ref()).await?
        }
        Ok(())
    }

    pub async fn get_pair(&self, key: &str) -> Result<Option<Pair>> {
        let query = self
            .client
            .query_documents(Query::new(self.get_query(key)))
            .query_cross_partition(false)
            .max_item_count(1);

        // There can be no duplicated keys, so we create the stream and only take the first result.
        let mut stream = query.into_stream::<Pair>();
        let Some(res) = stream.next().await else {
            return Ok(None);
        };
        Ok(res
            .map_err(log_error)?
            .results
            .first()
            .map(|(p, _)| p.clone()))
    }

    pub async fn get_keys(&self) -> Result<Vec<String>> {
        let query = self
            .client
            .query_documents(Query::new(self.get_keys_query()))
            .query_cross_partition(true);
        let mut res = Vec::new();

        let mut stream = query.into_stream::<Pair>();
        while let Some(resp) = stream.next().await {
            let resp = resp.map_err(log_error)?;
            res.extend(resp.results.into_iter().map(|(pair, _)| pair.id));
        }

        Ok(res)
    }

    fn get_query(&self, key: &str) -> String {
        let mut query = format!("SELECT * FROM c WHERE c.id='{}'", key);
        self.append_store_id(&mut query, true);
        query
    }

    fn get_keys_query(&self) -> String {
        let mut query = "SELECT * FROM c".to_owned();
        self.append_store_id(&mut query, false);
        query
    }

    fn get_in_query(&self, keys: Vec<String>) -> String {
        let in_clause: String = keys
            .into_iter()
            .map(|k| format!("'{k}'"))
            .collect::<Vec<String>>()
            .join(", ");

        let mut query = format!("SELECT * FROM c WHERE c.id IN ({})", in_clause);
        self.append_store_id(&mut query, true);
        query
    }

    fn append_store_id(&self, query: &mut String, condition_already_exists: bool) {
        append_store_id_condition(query, self.store_id.as_deref(), condition_already_exists);
    }
}

/// Appends an option store id condition to the query.
fn append_store_id_condition(
    query: &mut String,
    store_id: Option<&str>,
    condition_already_exists: bool,
) {
    if let Some(s) = store_id {
        if condition_already_exists {
            query.push_str(" AND");
        } else {
            query.push_str(" WHERE");
        }
        query.push_str(" c.store_id='");
        query.push_str(s);
        query.push('\'')
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Pair {
    pub id: String,
    pub value: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_id: Option<String>,
}

impl CosmosEntity for Pair {
    type Entity = String;

    fn partition_key(&self) -> Self::Entity {
        self.store_id.clone().unwrap_or_else(|| self.id.clone())
    }
}
pub fn log_error(err: impl std::fmt::Debug) -> anyhow::Error {
    println!("key-value error: {err:?}");
    anyhow::Error::msg("{err:?}")
}
