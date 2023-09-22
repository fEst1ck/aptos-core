import { Network } from "../utils/api-endpoints";

/**
 * Hex data as input to a function
 */
export type HexInput = string | Uint8Array;

/**
 * BCS types
 */
export type Uint8 = number;
export type Uint16 = number;
export type Uint32 = number;
export type Uint64 = bigint;
export type Uint128 = bigint;
export type Uint256 = bigint;
export type AnyNumber = number | bigint;

/**
 * Set of configuration options that can be provided when initializing the SDK.
 * The purpose of these options is to configure various aspects of the SDK's
 * behavior and interaction with the Aptos network
 */
export type AptosSettings = {
  readonly network: Network;

  readonly fullnode?: string;

  readonly faucet?: string;

  readonly indexer?: string;

  readonly clientConfig?: ClientConfig;
};

/**
 *
 * Controls the number of results that are returned and the starting position of those results.
 * @param start parameter specifies the starting position of the query result within the set of data. Default is 0.
 * @param limit specifies the maximum number of items or records to return in a query result. Default is 25.
 */
export interface PaginationArgs {
  start?: AnyNumber;
  limit?: number;
}

export interface IndexerPaginationArgs {
  offset?: AnyNumber;
  limit?: number;
}

/**
 * QUERY TYPES
 */

/**
 * A configuration object we can pass with the request to the server.
 *
 * @param TOKEN - an auth token to send with the request
 * @param HEADERS - extra headers we want to send with the request
 * @param WITH_CREDENTIALS - whether to carry cookies. By default, it is set to true and cookies will be sent
 */
export type ClientConfig = {
  TOKEN?: string;
  HEADERS?: Record<string, string | number | boolean>;
  WITH_CREDENTIALS?: boolean;
};

/**
 * The API request type
 *
 * @param url - the url to make the request to, i.e https://fullnode.aptoslabs.devnet.com/v1
 * @param method - the request method "GET" | "POST"
 * @param endpoint (optional) - the endpoint to make the request to, i.e transactions
 * @param body (optional) - the body of the request
 * @param contentType (optional) - the content type to set the `content-type` header to,
 * by default is set to `application/json`
 * @param params (optional) - query params to add to the request
 * @param originMethod (optional) - the local method the request came from
 * @param overrides (optional) - a `ClientConfig` object type to override request data
 */
export type AptosRequest = {
  url: string;
  method: "GET" | "POST";
  endpoint?: string;
  body?: any;
  contentType?: string;
  params?: Record<string, string | AnyNumber | boolean | undefined>;
  originMethod?: string;
  overrides?: ClientConfig;
};

/**
 * Specifies ledger version of transactions. By default latest version will be used
 */
export type LedgerVersion = {
  ledgerVersion?: AnyNumber;
};

/**
 * RESPONSE TYPES
 */

export type MoveResource = {
  type: MoveResourceType;
  data: {};
};

export type AccountData = {
  sequence_number: string;
  authentication_key: string;
};

export type MoveModuleBytecode = {
  bytecode: string;
  abi?: MoveModule;
};

export type GetAccountTokensCountQueryResponse = {
  __typename?: "query_root";
  current_token_ownerships_v2_aggregate: {
    __typename?: "current_token_ownerships_v2_aggregate";
    aggregate?: { __typename?: "current_token_ownerships_v2_aggregate_fields"; count: number } | null;
  };
};

export type GetAccountTokensCountQueryResult = number | undefined;

export type GetOwnedTokensQueryResponse = {
  __typename?: "query_root";
  current_token_ownerships_v2: Array<{
    __typename?: "current_token_ownerships_v2";
    token_standard: string;
    token_properties_mutated_v1?: any | null;
    token_data_id: string;
    table_type_v1?: string | null;
    storage_id: string;
    property_version_v1: any;
    owner_address: string;
    last_transaction_version: any;
    last_transaction_timestamp: any;
    is_soulbound_v2?: boolean | null;
    is_fungible_v2?: boolean | null;
    amount: any;
    current_token_data?: {
      __typename?: "current_token_datas_v2";
      collection_id: string;
      description: string;
      is_fungible_v2?: boolean | null;
      largest_property_version_v1?: any | null;
      last_transaction_timestamp: any;
      last_transaction_version: any;
      maximum?: any | null;
      supply: any;
      token_data_id: string;
      token_name: string;
      token_properties: any;
      token_standard: string;
      token_uri: string;
      current_collection?: {
        __typename?: "current_collections_v2";
        collection_id: string;
        collection_name: string;
        creator_address: string;
        current_supply: any;
        description: string;
        last_transaction_timestamp: any;
        last_transaction_version: any;
        max_supply?: any | null;
        mutable_description?: boolean | null;
        mutable_uri?: boolean | null;
        table_handle_v1?: string | null;
        token_standard: string;
        total_minted_v2?: any | null;
        uri: string;
      } | null;
    } | null;
  }>;
};

/**
 * Holds a generic type that being passed by each function and holds an
 * array of properties we can sort the query by
 */
export type IndexerSortBy<T> = IndexerSortingOptions<T>;
export type InputMaybe<T> = T | null;
export type IndexerSortingOptions<T> = {
  [K in keyof T]?: T[K] extends InputMaybe<infer U> ? IndexerSortingOptions<U> | U | OrderBy : T[K] | OrderBy;
};

export enum OrderBy {
  /** in ascending order, nulls last */
  Asc = "asc",
  /** in ascending order, nulls first */
  AscNullsFirst = "asc_nulls_first",
  /** in ascending order, nulls last */
  AscNullsLast = "asc_nulls_last",
  /** in descending order, nulls first */
  Desc = "desc",
  /** in descending order, nulls first */
  DescNullsFirst = "desc_nulls_first",
  /** in descending order, nulls last */
  DescNullsLast = "desc_nulls_last",
}

/**
 * Refers to the token standard we want to query for
 */
export type TokenStandard = "v1" | "v2";

/**
 * TRANSACTION TYPES
 */

export type TransactionResponse =
  | PendingTransactionResponse
  | UserTransactionResponse
  | GenesisTransactionResponse
  | BlockMetadataTransactionResponse
  | StateCheckpointTransactionResponse;

export type PendingTransactionResponse = {
  type: string;
  hash: string;
  sender: string;
  sequence_number: string;
  max_gas_amount: string;
  gas_unit_price: string;
  expiration_timestamp_secs: string;
  payload: TransactionPayload;
  signature?: TransactionSignature;
};

export type UserTransactionResponse = {
  type: string;
  version: string;
  hash: string;
  state_change_hash: string;
  event_root_hash: string;
  state_checkpoint_hash?: string;
  gas_used: string;
  /**
   * Whether the transaction was successful
   */
  success: boolean;
  /**
   * The VM status of the transaction, can tell useful information in a failure
   */
  vm_status: string;
  accumulator_root_hash: string;
  /**
   * Final state of resources changed by the transaction
   */
  changes: Array<WriteSetChange>;
  sender: string;
  sequence_number: string;
  max_gas_amount: string;
  gas_unit_price: string;
  expiration_timestamp_secs: string;
  payload: TransactionPayload;
  signature?: TransactionSignature;
  /**
   * Events generated by the transaction
   */
  events: Array<Event>;
  timestamp: string;
};

export type GenesisTransactionResponse = {
  type: string;
  version: string;
  hash: string;
  state_change_hash: string;
  event_root_hash: string;
  state_checkpoint_hash?: string;
  gas_used: string;
  /**
   * Whether the transaction was successful
   */
  success: boolean;
  /**
   * The VM status of the transaction, can tell useful information in a failure
   */
  vm_status: string;
  accumulator_root_hash: string;
  /**
   * Final state of resources changed by the transaction
   */
  changes: Array<WriteSetChange>;
  payload: GenesisPayload;
  /**
   * Events emitted during genesis
   */
  events: Array<Event>;
};

export type BlockMetadataTransactionResponse = {
  type: string;
  version: string;
  hash: string;
  state_change_hash: string;
  event_root_hash: string;
  state_checkpoint_hash?: string;
  gas_used: string;
  /**
   * Whether the transaction was successful
   */
  success: boolean;
  /**
   * The VM status of the transaction, can tell useful information in a failure
   */
  vm_status: string;
  accumulator_root_hash: string;
  /**
   * Final state of resources changed by the transaction
   */
  changes: Array<WriteSetChange>;
  id: string;
  epoch: string;
  round: string;
  /**
   * The events emitted at the block creation
   */
  events: Array<Event>;
  /**
   * Previous block votes
   */
  previous_block_votes_bitvec: Array<number>;
  proposer: string;
  /**
   * The indices of the proposers who failed to propose
   */
  failed_proposer_indices: Array<number>;
  timestamp: string;
};

export type StateCheckpointTransactionResponse = {
  type: string;
  version: string;
  hash: string;
  state_change_hash: string;
  event_root_hash: string;
  state_checkpoint_hash?: string;
  gas_used: string;
  /**
   * Whether the transaction was successful
   */
  success: boolean;
  /**
   * The VM status of the transaction, can tell useful information in a failure
   */
  vm_status: string;
  accumulator_root_hash: string;
  /**
   * Final state of resources changed by the transaction
   */
  changes: Array<WriteSetChange>;
  timestamp: string;
};

/**
 * WRITESET CHANGE TYPES
 */

export type WriteSetChange =
  | WriteSetChangeDeleteModule
  | WriteSetChangeDeleteResource
  | WriteSetChangeDeleteTableItem
  | WriteSetChangeWriteModule
  | WriteSetChangeWriteResource
  | WriteSetChangeWriteTableItem;

export type WriteSetChangeDeleteModule = {
  type: string;
  address: string;
  /**
   * State key hash
   */
  state_key_hash: string;
  module: MoveModuleId;
};

export type WriteSetChangeDeleteResource = {
  type: string;
  address: string;
  state_key_hash: string;
  resource: string;
};

export type WriteSetChangeDeleteTableItem = {
  type: string;
  state_key_hash: string;
  handle: string;
  key: string;
  data?: DeletedTableData;
};

export type WriteSetChangeWriteModule = {
  type: string;
  address: string;
  state_key_hash: string;
  data: MoveModuleBytecode;
};

export type WriteSetChangeWriteResource = {
  type: string;
  address: string;
  state_key_hash: string;
  data: MoveResource;
};

export type WriteSetChangeWriteTableItem = {
  type: string;
  state_key_hash: string;
  handle: string;
  key: string;
  value: string;
  data?: DecodedTableData;
};

export type DecodedTableData = {
  /**
   * Key of table in JSON
   */
  key: any;
  /**
   * Type of key
   */
  key_type: string;
  /**
   * Value of table in JSON
   */
  value: any;
  /**
   * Type of value
   */
  value_type: string;
};

/**
 * Deleted table data
 */
export type DeletedTableData = {
  /**
   * Deleted key
   */
  key: any;
  /**
   * Deleted key type
   */
  key_type: string;
};

export type TransactionPayload = EntryFunctionPayload | ScriptPayload | MultisigPayload;

export type EntryFunctionPayload = {
  type: string;
  function: MoveResourceType;
  /**
   * Type arguments of the function
   */
  type_arguments: Array<string>;
  /**
   * Arguments of the function
   */
  arguments: Array<any>;
};

export type ScriptPayload = {
  type: string;
  code: MoveScriptBytecode;
  /**
   * Type arguments of the function
   */
  type_arguments: Array<string>;
  /**
   * Arguments of the function
   */
  arguments: Array<any>;
};

export type MultisigPayload = {
  type: string;
  multisig_address: string;
  transaction_payload?: EntryFunctionPayload;
};

export type GenesisPayload = {
  type: string;
  write_set: WriteSet;
};

/**
 * Move script bytecode
 */
export type MoveScriptBytecode = {
  bytecode: string;
  abi?: MoveFunction;
};

export type TransactionSignature =
  | TransactionEd25519Signature
  | TransactionMultiEd25519Signature
  | TransactionMultiAgentSignature
  | TransactioneePayerSignature;

export type TransactionEd25519Signature = {
  type: string;
  public_key: string;
  signature: string;
};

export type TransactionMultiEd25519Signature = {
  type: string;
  /**
   * The public keys for the Ed25519 signature
   */
  public_keys: Array<string>;
  /**
   * Signature associated with the public keys in the same order
   */
  signatures: Array<string>;
  /**
   * The number of signatures required for a successful transaction
   */
  threshold: number;
  bitmap: string;
};

export type TransactionMultiAgentSignature = {
  type: string;
  sender: AccountSignature;
  /**
   * The other involved parties' addresses
   */
  secondary_signer_addresses: Array<string>;
  /**
   * The associated signatures, in the same order as the secondary addresses
   */
  secondary_signers: Array<AccountSignature>;
};

export type TransactioneePayerSignature = {
  type: string;
  sender: AccountSignature;
  /**
   * The other involved parties' addresses
   */
  secondary_signer_addresses: Array<string>;
  /**
   * The associated signatures, in the same order as the secondary addresses
   */
  secondary_signers: Array<AccountSignature>;
  fee_payer_address: string;
  fee_payer_signer: AccountSignature;
};

export type AccountSignature = AccountEd25519Signature | AccountMultiEd25519Signature;

export type AccountEd25519Signature = {
  type: string;
  public_key: string;
  signature: string;
};

export type AccountMultiEd25519Signature = {
  type: string;
  /**
   * The public keys for the Ed25519 signature
   */
  public_keys: Array<string>;
  /**
   * Signature associated with the public keys in the same order
   */
  signatures: Array<string>;
  /**
   * The number of signatures required for a successful transaction
   */
  threshold: number;
  bitmap: string;
};

export type WriteSet = ScriptWriteSet | DirectWriteSet;

export type ScriptWriteSet = {
  type: string;
  execute_as: string;
  script: ScriptPayload;
};

export type DirectWriteSet = {
  type: string;
  changes: Array<WriteSetChange>;
  events: Array<Event>;
};

export type EventGuid = {
  creation_number: string;
  account_address: string;
};

export type Event = {
  guid: EventGuid;
  sequence_number: string;
  type: string;
  /**
   * The JSON representation of the event
   */
  data: any;
};

/**
 * Map of Move types to local TypeScript types
 */
export type MoveUint8Type = number;
export type MoveUint16Type = number;
export type MoveUint32Type = number;
export type MoveUint64Type = string;
export type MoveUint128Type = string;
export type MoveUint256Type = string;
export type MoveAddressType = `0x${string}`;
export type MoveObjectType = `0x${string}`;
export type MoveStructType = `0x${string}::${string}::${string}`;
export type MoveOptionType = MoveType | null | undefined;
/**
 * String representation of a on-chain Move struct type.
 */
export type MoveResourceType = `${string}::${string}::${string}`;

export type MoveType =
  | boolean
  | string
  | MoveUint8Type
  | MoveUint16Type
  | MoveUint32Type
  | MoveUint64Type
  | MoveUint128Type
  | MoveUint256Type
  | MoveAddressType
  | MoveObjectType
  | MoveStructType
  | Array<MoveType>;

/**
 * Possible Move values acceptable by move functions (entry, view)
 *
 * `Bool -> boolean`
 *
 * `u8, u16, u32 -> number`
 *
 * `u64, u128, u256 -> string`
 *
 * `String -> string`
 *
 * `Address -> 0x${string}`
 *
 * `Struct - 0x${string}::${string}::${string}`
 *
 * `Object -> 0x${string}`
 *
 * `Vector -> Array<MoveValue>`
 *
 * `Option -> MoveValue | null | undefined`
 */
export type MoveValue =
  | boolean
  | string
  | MoveUint8Type
  | MoveUint16Type
  | MoveUint32Type
  | MoveUint64Type
  | MoveUint128Type
  | MoveUint256Type
  | MoveAddressType
  | MoveObjectType
  | MoveStructType
  | MoveOptionType
  | Array<MoveValue>;

/**
 * Move module id is a string representation of Move module.
 * Module name is case-sensitive.
 */
export type MoveModuleId = `${string}::${string}`;

/**
 * Move function visibility
 */
export enum MoveFunctionVisibility {
  PRIVATE = "private",
  PUBLIC = "public",
  FRIEND = "friend",
}

/**
 * Move function ability
 */
export enum MoveAbility {
  STORE = "store",
  DROP = "drop",
  KEY = "key",
  COPY = "copy",
}

/**
 * Move abilities tied to the generic type param and associated with the function that uses it
 */
export type MoveFunctionGenericTypeParam = {
  constraints: Array<MoveAbility>;
};

/**
 * Move struct field
 */
export type MoveStructField = {
  name: string;
  type: string;
};

/**
 * A Move module
 */
export type MoveModule = {
  address: string;
  name: string;
  /**
   * Friends of the module
   */
  friends: Array<MoveModuleId>;
  /**
   * Public functions of the module
   */
  exposed_functions: Array<MoveFunction>;
  /**
   * Structs of the module
   */
  structs: Array<MoveStruct>;
};

/**
 * A move struct
 */
export type MoveStruct = {
  name: string;
  /**
   * Whether the struct is a native struct of Move
   */
  is_native: boolean;
  /**
   * Abilities associated with the struct
   */
  abilities: Array<MoveAbility>;
  /**
   * Generic types associated with the struct
   */
  generic_type_params: Array<MoveFunctionGenericTypeParam>;
  /**
   * Fields associated with the struct
   */
  fields: Array<MoveStructField>;
};

/**
 * Move function
 */
export type MoveFunction = {
  name: string;
  visibility: MoveFunctionVisibility;
  /**
   * Whether the function can be called as an entry function directly in a transaction
   */
  is_entry: boolean;
  /**
   * Whether the function is a view function or not
   */
  is_view: boolean;
  /**
   * Generic type params associated with the Move function
   */
  generic_type_params: Array<MoveFunctionGenericTypeParam>;
  /**
   * Parameters associated with the move function
   */
  params: Array<string>;
  /**
   * Return type of the function
   */
  return: Array<string>;
};

export enum RoleType {
  VALIDATOR = "validator",
  FULL_NODE = "full_node",
}

export type LedgerInfo = {
  /**
   * Chain ID of the current chain
   */
  chain_id: number;
  epoch: string;
  ledger_version: string;
  oldest_ledger_version: string;
  ledger_timestamp: string;
  node_role: RoleType;
  oldest_block_height: string;
  block_height: string;
  /**
   * Git hash of the build of the API endpoint.  Can be used to determine the exact
   * software version used by the API endpoint.
   */
  git_hash?: string;
};

/**
 * A Block type
 */
export type Block = {
  block_height: string;
  block_hash: `0x${string}`;
  block_timestamp: string;
  first_version: string;
  last_version: string;
  /**
   * The transactions in the block in sequential order
   */
  transactions?: Array<TransactionResponse>;
};

/////// REQUEST TYPES ///////

/**
 * View request for the Move view function API
 *
 * `type MoveResourceType = ${string}::${string}::${string}`;
 */
export type ViewRequest = {
  function: MoveResourceType;
  /**
   * Type arguments of the function
   */
  type_arguments: Array<MoveResourceType>;
  /**
   * Arguments of the function
   */
  arguments: Array<MoveValue>;
};

/**
 * Table Item request for the GetTableItem API
 */
export type TableItemRequest = {
  key_type: MoveValue;
  value_type: MoveValue;
  /**
   * The value of the table item's key
   */
  key: any;
};
