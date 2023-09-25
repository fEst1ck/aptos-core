export const GetAccountCoinsCount = /* GraphQL */ `
  query getAccountCoinsDataCount($address: String) {
    current_fungible_asset_balances_aggregate(where: { owner_address: { _eq: $address } }) {
      aggregate {
        count
      }
    }
  }
`;
