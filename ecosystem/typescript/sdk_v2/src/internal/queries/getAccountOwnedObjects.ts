export const GetAccountOwnedObjects = /* GraphQL */ `
  query getCurrentObjects(
    $where_condition: current_objects_bool_exp
    $offset: Int
    $limit: Int
    $order_by: [current_objects_order_by!]
  ) {
    current_objects(where: $where_condition, offset: $offset, limit: $limit, order_by: $order_by) {
      allow_ungated_transfer
      state_key_hash
      owner_address
      object_address
      last_transaction_version
      last_guid_creation_num
      is_deleted
    }
  }
`;
