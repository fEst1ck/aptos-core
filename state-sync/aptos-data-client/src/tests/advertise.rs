// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    error::Error,
    interface::AptosDataClientInterface,
    peer_states::calculate_optimal_chunk_sizes,
    poller,
    tests::{mock::MockNetwork, utils},
};
use aptos_config::{config::AptosDataClientConfig, network_id::NetworkId};
use aptos_network::protocols::wire::handshake::v1::ProtocolId;
use aptos_storage_service_server::network::NetworkRequest;
use aptos_storage_service_types::{
    requests::{DataRequest, TransactionsWithProofRequest},
    responses::{CompleteDataRange, DataResponse, StorageServiceResponse},
};
use aptos_time_service::MockTimeService;
use aptos_types::transaction::TransactionListWithProof;
use claims::assert_matches;
use std::time::Duration;

#[tokio::test]
async fn request_works_only_when_data_available() {
    // Ensure the properties hold for both priority and non-priority peers
    for poll_priority_peers in [true, false] {
        // Create the mock network, client and poller
        let data_client_config = AptosDataClientConfig::default();
        let (mut mock_network, mut mock_time, client, poller) =
            MockNetwork::new(None, Some(data_client_config), None);

        // Start the poller
        tokio::spawn(poller::start_poller(poller));

        // This request should fail because no peers are currently connected
        let request_timeout = data_client_config.response_timeout_ms;
        let error = client
            .get_transactions_with_proof(100, 50, 100, false, request_timeout)
            .await
            .unwrap_err();
        assert_matches!(error, Error::DataIsUnavailable(_));

        // Add a connected peer
        let peer = mock_network.add_peer(poll_priority_peers);
        let network_id = peer.network_id();

        // Verify the peer's state has not been updated
        let peer_states = client.get_peer_states();
        let peer_to_states = peer_states.get_peer_to_states();
        assert!(peer_to_states.is_empty());

        // Requesting some txns now will still fail since no peers are advertising
        // availability for the desired range.
        let error = client
            .get_transactions_with_proof(100, 50, 100, false, request_timeout)
            .await
            .unwrap_err();
        assert_matches!(error, Error::DataIsUnavailable(_));

        // Advance time so the poller sends a data summary request
        let poll_loop_interval_ms = data_client_config.data_poller_config.poll_loop_interval_ms;
        advance_polling_timer(&mut mock_time, poll_loop_interval_ms).await;

        // Verify the received network request
        let network_request = mock_network.next_request(network_id).await.unwrap();
        assert_eq!(network_request.peer_network_id, peer);
        assert_eq!(network_request.protocol_id, ProtocolId::StorageServiceRpc);
        assert!(network_request.storage_service_request.use_compression);
        assert_matches!(
            network_request.storage_service_request.data_request,
            DataRequest::GetStorageServerSummary
        );

        // Fulfill the request
        let storage_summary = utils::create_storage_summary(200);
        let data_response = DataResponse::StorageServerSummary(storage_summary.clone());
        network_request
            .response_sender
            .send(Ok(StorageServiceResponse::new(data_response, true).unwrap()));

        // Let the poller finish processing the response
        tokio::task::yield_now().await;

        // Handle the client's transactions request
        tokio::spawn(async move {
            // Verify the received network request
            let network_request = get_next_network_request(&mut mock_network, network_id).await;
            assert_eq!(network_request.peer_network_id, peer);
            assert_eq!(network_request.protocol_id, ProtocolId::StorageServiceRpc);
            assert!(network_request.storage_service_request.use_compression);
            assert_matches!(
                network_request.storage_service_request.data_request,
                DataRequest::GetTransactionsWithProof(TransactionsWithProofRequest {
                    start_version: 50,
                    end_version: 100,
                    proof_version: 100,
                    include_events: false,
                })
            );

            // Fulfill the request
            let data_response =
                DataResponse::TransactionsWithProof(TransactionListWithProof::new_empty());
            network_request
                .response_sender
                .send(Ok(StorageServiceResponse::new(data_response, true).unwrap()));
        });

        // Verify the peer's state has been updated
        let peer_state = peer_to_states.get(&peer).unwrap().value().clone();
        let peer_storage_summary = peer_state
            .get_storage_summary_if_not_ignored()
            .unwrap()
            .clone();
        assert_eq!(peer_storage_summary, storage_summary);

        // The client's request should succeed since a peer finally has advertised
        // data for this range.
        let response = client
            .get_transactions_with_proof(100, 50, 100, false, request_timeout)
            .await
            .unwrap();
        assert_eq!(response.payload, TransactionListWithProof::new_empty());
    }
}

#[tokio::test]
async fn update_global_data_summary() {
    // Create the mock network, client and poller
    let data_client_config = AptosDataClientConfig::default();
    let (mut mock_network, mut mock_time, client, poller) =
        MockNetwork::new(None, Some(data_client_config), None);

    // Start the poller
    tokio::spawn(poller::start_poller(poller));

    // Verify the global data summary is empty
    let global_data_summary = client.get_global_data_summary();
    assert!(global_data_summary.is_empty());

    // Add a priority peer
    let priority_peer = mock_network.add_peer(true);
    let priority_network = priority_peer.network_id();

    // Advance time so the poller sends a data summary request for the peer
    let poll_loop_interval_ms = data_client_config.data_poller_config.poll_loop_interval_ms;
    advance_polling_timer(&mut mock_time, poll_loop_interval_ms).await;

    // Handle the priority peer's data summary request
    let network_request = get_next_network_request(&mut mock_network, priority_network).await;
    let priority_peer_version = 10_000;
    let priority_storage_summary = utils::create_storage_summary(priority_peer_version);
    let data_response = DataResponse::StorageServerSummary(priority_storage_summary.clone());
    network_request
        .response_sender
        .send(Ok(StorageServiceResponse::new(data_response, true).unwrap()));

    // Advance time so the poller sends a data summary request for the peer
    advance_polling_timer(&mut mock_time, poll_loop_interval_ms).await;

    // Verify that the advertised data ranges are valid
    let global_data_summary = client.get_global_data_summary();
    let advertised_data = global_data_summary.advertised_data;
    assert_eq!(
        advertised_data.transactions[0],
        CompleteDataRange::new(0, priority_peer_version).unwrap()
    );

    // Verify that the highest synced ledger info is valid
    let highest_synced_ledger_info = advertised_data.highest_synced_ledger_info().unwrap();
    assert_eq!(
        highest_synced_ledger_info.ledger_info().version(),
        priority_peer_version
    );

    // Add a regular peer
    let regular_peer = mock_network.add_peer(false);
    let regular_network = regular_peer.network_id();

    // Advance time so the poller sends a data summary request for both peers
    advance_polling_timer(&mut mock_time, poll_loop_interval_ms).await;

    // Handle the priority peer's data summary request
    let network_request = get_next_network_request(&mut mock_network, priority_network).await;
    let priority_peer_version = 20_000;
    let priority_storage_summary = utils::create_storage_summary(priority_peer_version);
    let data_response = DataResponse::StorageServerSummary(priority_storage_summary.clone());
    network_request
        .response_sender
        .send(Ok(StorageServiceResponse::new(data_response, true).unwrap()));

    // Handle the regular peer's data summary request (using more data)
    let network_request = get_next_network_request(&mut mock_network, regular_network).await;
    let regular_peer_version = 30_000;
    let regular_storage_summary = utils::create_storage_summary(regular_peer_version);
    let data_response = DataResponse::StorageServerSummary(regular_storage_summary.clone());
    network_request
        .response_sender
        .send(Ok(StorageServiceResponse::new(data_response, true).unwrap()));

    // Advance time so the poller elapses
    advance_polling_timer(&mut mock_time, poll_loop_interval_ms).await;

    // Verify that the advertised data ranges are valid
    let global_data_summary = client.get_global_data_summary();
    let advertised_data = global_data_summary.advertised_data;
    assert_eq!(advertised_data.transactions.len(), 2);
    assert!(advertised_data
        .transactions
        .contains(&CompleteDataRange::new(0, priority_peer_version).unwrap()));
    assert!(advertised_data
        .transactions
        .contains(&CompleteDataRange::new(0, regular_peer_version).unwrap()));

    // Verify that the highest synced ledger info is valid
    let highest_synced_ledger_info = advertised_data.highest_synced_ledger_info().unwrap();
    assert_eq!(
        highest_synced_ledger_info.ledger_info().version(),
        regular_peer_version
    );
}

#[tokio::test]
async fn update_peer_states() {
    // Create the mock network, client and poller
    let data_client_config = AptosDataClientConfig::default();
    let (mut mock_network, mut mock_time, client, poller) =
        MockNetwork::new(None, Some(data_client_config), None);

    // Start the poller
    tokio::spawn(poller::start_poller(poller));

    // Add a priority peer
    let priority_peer = mock_network.add_peer(true);
    let priority_network = priority_peer.network_id();

    // Verify that we have no peer states
    let peer_states = client.get_peer_states();
    let peer_to_states = peer_states.get_peer_to_states();
    assert!(peer_to_states.is_empty());

    // Advance time so the poller sends a data summary request for the peer
    let poll_loop_interval_ms = data_client_config.data_poller_config.poll_loop_interval_ms;
    advance_polling_timer(&mut mock_time, poll_loop_interval_ms).await;

    // Verify the priority peer's data summary request
    let network_request = get_next_network_request(&mut mock_network, priority_network).await;
    assert_matches!(
        network_request.storage_service_request.data_request,
        DataRequest::GetStorageServerSummary
    );

    // Handle the priority peer's data summary request
    let priority_storage_summary = utils::create_storage_summary(1111);
    let data_response = DataResponse::StorageServerSummary(priority_storage_summary.clone());
    network_request
        .response_sender
        .send(Ok(StorageServiceResponse::new(data_response, true).unwrap()));

    // Let the poller finish processing the responses
    tokio::task::yield_now().await;

    // Verify that the priority peer's state has been updated
    let peer_states = client.get_peer_states();
    let peer_to_states = peer_states.get_peer_to_states();
    let priority_peer_state = peer_to_states.get(&priority_peer).unwrap().value().clone();
    let priority_peer_summary = priority_peer_state
        .get_storage_summary_if_not_ignored()
        .unwrap()
        .clone();
    assert_eq!(priority_peer_summary, priority_storage_summary);

    // Add a regular peer
    let regular_peer = mock_network.add_peer(false);
    let regular_network = regular_peer.network_id();

    // Advance time so the poller sends a data summary request for both peers
    advance_polling_timer(&mut mock_time, poll_loop_interval_ms).await;

    // Handle the priority peer's data summary request
    let network_request = get_next_network_request(&mut mock_network, priority_network).await;
    let priority_storage_summary = utils::create_storage_summary(2222);
    let data_response = DataResponse::StorageServerSummary(priority_storage_summary.clone());
    network_request
        .response_sender
        .send(Ok(StorageServiceResponse::new(data_response, true).unwrap()));

    // Verify the regular peer's data summary request
    let network_request = get_next_network_request(&mut mock_network, regular_network).await;
    assert_matches!(
        network_request.storage_service_request.data_request,
        DataRequest::GetStorageServerSummary
    );

    // Handle the regular peer's data summary request
    let regular_storage_summary = utils::create_storage_summary(3333);
    let data_response = DataResponse::StorageServerSummary(regular_storage_summary.clone());
    network_request
        .response_sender
        .send(Ok(StorageServiceResponse::new(data_response, true).unwrap()));

    // Let the poller finish processing the responses
    tokio::task::yield_now().await;

    // Verify that the priority peer's state has been updated
    let peer_states = client.get_peer_states();
    let peer_to_states = peer_states.get_peer_to_states();
    let priority_peer_state = peer_to_states.get(&priority_peer).unwrap().value().clone();
    let priority_peer_summary = priority_peer_state
        .get_storage_summary_if_not_ignored()
        .unwrap()
        .clone();
    assert_eq!(priority_peer_summary, priority_storage_summary);

    // Verify that the regular peer's state has been set
    let regular_peer_state = peer_to_states.get(&regular_peer).unwrap().value().clone();
    let regular_peer_summary = regular_peer_state
        .get_storage_summary_if_not_ignored()
        .unwrap()
        .clone();
    assert_eq!(regular_peer_summary, regular_storage_summary);
}

#[tokio::test]
async fn optimal_chunk_size_calculations() {
    // Create a test storage service config
    let max_epoch_chunk_size = 600;
    let max_state_chunk_size = 500;
    let max_transaction_chunk_size = 700;
    let max_transaction_output_chunk_size = 800;
    let data_client_config = AptosDataClientConfig {
        max_epoch_chunk_size,
        max_state_chunk_size,
        max_transaction_chunk_size,
        max_transaction_output_chunk_size,
        ..Default::default()
    };

    // Test median calculations
    let optimal_chunk_sizes = calculate_optimal_chunk_sizes(
        &data_client_config,
        vec![7, 5, 6, 8, 10],
        vec![100, 200, 300, 100],
        vec![900, 700, 500],
        vec![40],
    );
    assert_eq!(200, optimal_chunk_sizes.state_chunk_size);
    assert_eq!(7, optimal_chunk_sizes.epoch_chunk_size);
    assert_eq!(700, optimal_chunk_sizes.transaction_chunk_size);
    assert_eq!(40, optimal_chunk_sizes.transaction_output_chunk_size);

    // Test no advertised data
    let optimal_chunk_sizes =
        calculate_optimal_chunk_sizes(&data_client_config, vec![], vec![], vec![], vec![]);
    assert_eq!(max_state_chunk_size, optimal_chunk_sizes.state_chunk_size);
    assert_eq!(max_epoch_chunk_size, optimal_chunk_sizes.epoch_chunk_size);
    assert_eq!(
        max_transaction_chunk_size,
        optimal_chunk_sizes.transaction_chunk_size
    );
    assert_eq!(
        max_transaction_output_chunk_size,
        optimal_chunk_sizes.transaction_output_chunk_size
    );

    // Verify the config caps the amount of chunks
    let optimal_chunk_sizes = calculate_optimal_chunk_sizes(
        &data_client_config,
        vec![70, 50, 60, 80, 100],
        vec![1000, 1000, 2000, 3000],
        vec![9000, 7000, 5000],
        vec![400],
    );
    assert_eq!(max_state_chunk_size, optimal_chunk_sizes.state_chunk_size);
    assert_eq!(70, optimal_chunk_sizes.epoch_chunk_size);
    assert_eq!(
        max_transaction_chunk_size,
        optimal_chunk_sizes.transaction_chunk_size
    );
    assert_eq!(400, optimal_chunk_sizes.transaction_output_chunk_size);
}

/// Advances time by at least the polling loop interval
async fn advance_polling_timer(mock_time: &mut MockTimeService, poll_loop_interval_ms: u64) {
    for _ in 0..10 {
        tokio::task::yield_now().await;
        mock_time
            .advance_async(Duration::from_millis(poll_loop_interval_ms))
            .await;
    }
}

/// Returns the next network request for the given network id
async fn get_next_network_request(
    mock_network: &mut MockNetwork,
    network_id: NetworkId,
) -> NetworkRequest {
    mock_network.next_request(network_id).await.unwrap()
}
