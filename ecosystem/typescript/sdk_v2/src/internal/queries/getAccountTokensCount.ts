export const GetAccountTokensCount = /* GraphQL */ `
  query getAccountTokensCount($where_condition: current_token_ownerships_v2_bool_exp, $offset: Int, $limit: Int) {
    current_token_ownerships_v2_aggregate(where: $where_condition, offset: $offset, limit: $limit) {
      aggregate {
        count
      }
    }
  }
`;
