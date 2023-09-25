/**
 * QUERY TYPES COMING FROM HASURA
 *
 * Custom types we build that match the structure of the
 * response type when querying from Hasura schema.
 *
 * These types are used as the return type when making the actual request.
 */

export type GetAccountTokensCountQuery = {
  current_token_ownerships_v2_aggregate: {
    aggregate?: {
      count?: number;
    };
  };
};

export type GetAccountTransactionsCountQuery = {
  account_transactions_aggregate: {
    aggregate?: {
      count?: number;
    };
  };
};

export type GetAccountCoinsCountQuery = {
  current_fungible_asset_balances_aggregate: {
    aggregate?: {
      count?: number;
    };
  };
};

export type GetAccountCoinsDataQuery = {
  current_fungible_asset_balances: Array<{
    amount: number;
    asset_type: string;
    is_frozen: number;
    is_primary: number;
    last_transaction_timestamp: string;
    last_transaction_version: bigint;
    owner_address: string;
    storage_id: string;
    token_standard: "v1" | "v2";
    metadata: {
      asset_type: string;
      creator_address: string;
      decimals: number;
      icon_uri?: string;
      last_transaction_timestamp: string;
      last_transaction_version: bigint;
      name: string;
      project_uri?: string;
      supply_aggregator_table_handle_v1?: string;
      supply_aggregator_table_key_v1?: String;
      symbol: string;
      token_standard: "v1" | "v2";
    };
  }>;
};

export type GetAccountOwnedObjectsQuery = {
  current_objects: Array<{
    allow_ungated_transfer: boolean;
    is_deleted: boolean;
    last_guid_creation_num: number;
    last_transaction_version: bigint;
    object_address: string;
    owner_address: string;
    state_key_hash: string;
  }>;
};

export type GetAccountOwnedTokensByTokenDataQuery = CurrentTokenOwnership;
export type GetAccountCollectionsWithOwnedTokenQuery = CurrentCollectionOwnership;

/**
 * RESPONSE TYPES FOR THE END USER
 *
 * To provide a good dev exp, we build custom types derived from the
 * query types to be the response type the end developer/user will
 * work with.
 *
 * These types are used as the return type when calling an sdk api function
 * that queries the server.
 */
export type GetAccountTokensCountQueryResponse =
  GetAccountTokensCountQuery["current_token_ownerships_v2_aggregate"]["aggregate"];
export type GetAccountTransactionsCountResponse =
  GetAccountTransactionsCountQuery["account_transactions_aggregate"]["aggregate"];
export type GetAccountCoinsCountResponse =
  GetAccountCoinsCountQuery["current_fungible_asset_balances_aggregate"]["aggregate"];

export type GetAccountOwnedTokensQueryResponse = CurrentTokenOwnership["current_token_ownerships_v2"];

export type GetAccountOwnedTokensFromCollectionAddressResponse = CurrentTokenOwnership["current_token_ownerships_v2"];
export type GetAccountCollectionsWithOwnedTokenResponse =
  CurrentCollectionOwnership["current_collection_ownership_v2_view"];
export type GetAccountCoinsDataResponse = GetAccountCoinsDataQuery["current_fungible_asset_balances"];
export type GetAccountOwnedObjectsResponse = GetAccountOwnedObjectsQuery["current_objects"];

/**
 * Global types used by multiple different response query types
 */
export type CurrentTokenOwnership = {
  current_token_ownerships_v2: Array<{
    token_standard: "v1" | "v2";
    token_properties_mutated_v1?: any;
    token_data_id: string;
    table_type_v1?: `${string}::${string}::${string}`;
    storage_id: string;
    property_version_v1?: number;
    owner_address: string;
    last_transaction_version: bigint;
    last_transaction_timestamp: string;
    is_soulbound_v2: boolean;
    is_fungible_v2: boolean;
    amount: number;
    current_token_data: {
      collection_id: string;
      description: string;
      is_fungible_v2: boolean;
      largest_property_version_v1?: number;
      last_transaction_timestamp: string;
      last_transaction_version: bigint;
      maximum?: number;
      supply: number;
      token_data_id: string;
      token_name: string;
      token_properties: any;
      token_standard: "v1" | "v2";
      token_uri: string;
      current_collection: {
        collection_id: string;
        collection_name: string;
        creator_address: string;
        current_supply: number;
        description: string;
        last_transaction_timestamp: string;
        last_transaction_version: bigint;
        max_supply?: number;
        mutable_description?: boolean;
        mutable_uri?: boolean;
        table_handle_v1?: string;
        token_standard: "v1" | "v2";
        total_minted_v2?: number;
        uri: string;
      };
    };
  }>;
};

export type CurrentCollectionOwnership = {
  current_collection_ownership_v2_view: Array<{
    collection_id?: string;
    collection_name?: string;
    collection_uri?: string;
    creator_address?: string;
    distinct_tokens: bigint;
    last_transaction_version: bigint;
    owner_address?: string;
    single_token_uri?: string;
    current_collection?: {
      collection_id: string;
      collection_name: string;
      creator_address: string;
      current_supply: number;
      description: string;
      last_transaction_timestamp: string;
      last_transaction_version: bigint;
      max_supply?: number;
      mutable_description?: boolean;
      mutable_uri?: boolean;
      table_handle_v1?: string;
      token_standard: "v1" | "v2";
      total_minted_v2?: number;
      uri: string;
    };
  }>;
};

/**
 * A generic type that being passed by each function and holds an
 * array of properties we can sort the query by
 */
export type OrderBy<T> = Array<{ [K in keyof T]?: OrderByValue }>;
export type OrderByValue =
  | "asc"
  | "asc_nulls_first"
  | "asc_nulls_last"
  | "desc"
  | "desc_nulls_first"
  | "desc_nulls_last";

/**
 * Refers to the token standard we want to query for
 */
export type TokenStandard = "v1" | "v2";

/**
 *
 * Controls the number of results that are returned and the starting position of those results.
 * @param offset parameter specifies the starting position of the query result within the set of data. Default is 0.
 * @param limit specifies the maximum number of items or records to return in a query result. Default is 10.
 */
export interface IndexerPaginationArgs {
  offset?: number | bigint;
  limit?: number;
}

/**
 * The graphql query type to pass into the `queryIndexer` function
 */
export type GraphqlQuery = {
  query: string;
  variables?: {};
};
