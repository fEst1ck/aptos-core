export const GetAccountTransactionsCount = /* GraphQL */ `
  query getAccountTransactionsCount($address: String) {
    account_transactions_aggregate(where: { account_address: { _eq: $address } }) {
      aggregate {
        count
      }
    }
  }
`;
