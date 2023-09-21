// Copyright © Aptos Foundation

use aptos_protos::transaction::v1::Transaction;
use prost::Message;
use serde::{Deserialize, Serialize};

pub mod access_trait;
pub mod gcs;
pub mod in_memory;
pub mod in_memory_storage;
pub mod local_file;
pub mod redis;

use crate::access_trait::{
    AccessMetadata, StorageReadError, StorageReadStatus, StorageTransactionRead,
};

#[enum_dispatch::enum_dispatch]
#[derive(Clone)]
pub enum StorageClient {
    InMemory(in_memory::InMemoryStorageClient),
    Redis(redis::RedisClient),
    GCS(gcs::GcsClient),
    LocalFile(local_file::LocalFileClient),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "storage_type")]
pub enum ReadOnlyStorageType {
    InMemory(in_memory::InMemoryStorageClientConfig),
    Redis(redis::RedisClientConfig),
    GCS(gcs::GcsClientConfig),
    LocalFile(local_file::LocalFileClientConfig),
}

const REDIS_ENDING_VERSION_EXCLUSIVE_KEY: &str = "latest_version";
const REDIS_CHAIN_ID: &str = "chain_id";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
struct FileMetadata {
    pub chain_id: u64,
    pub file_folder_size: u64,
    pub version: u64,
}

impl From<Vec<u8>> for FileMetadata {
    fn from(bytes: Vec<u8>) -> Self {
        serde_json::from_slice(bytes.as_slice()).expect("Failed to deserialize FileMetadata.")
    }
}

type Based64EncodedSerializedTransactionProtobuf = String;

#[derive(Debug, Serialize, Deserialize)]
struct TransactionsFile {
    pub transactions: Vec<Based64EncodedSerializedTransactionProtobuf>,
    pub starting_version: u64,
}

impl From<Vec<u8>> for TransactionsFile {
    fn from(bytes: Vec<u8>) -> Self {
        serde_json::from_slice(bytes.as_slice()).expect("Failed to deserialize Transactions file.")
    }
}
impl From<TransactionsFile> for Vec<Transaction> {
    fn from(transactions_file: TransactionsFile) -> Self {
        transactions_file
            .transactions
            .into_iter()
            .map(|transaction| {
                let bytes = base64::decode(transaction).expect("Failed to decode base64.");
                Transaction::decode(bytes.as_slice()).expect("Failed to decode protobuf.")
            })
            .collect()
    }
}

#[inline]
fn get_transactions_file_name(version: u64) -> String {
    // This assumes that the transactions are stored in file of 1000 versions.
    format!("files/{}.json", version / 1000 * 1000)
}
