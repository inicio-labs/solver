mod test_types;
mod test_order_book;
mod test_direct_matching;
mod test_three_edge_cycle;
mod test_engine;
mod test_fuzz;
mod test_experimental;
mod test_experimental_v2;
mod test_debug_surplus;

use miden_protocol::account::AccountId;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_3,
    ACCOUNT_ID_NETWORK_FUNGIBLE_FAUCET,
};

fn eth() -> AccountId {
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap()
}

fn usdc() -> AccountId {
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap()
}

fn sol() -> AccountId {
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2.try_into().unwrap()
}

fn btc() -> AccountId {
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_3.try_into().unwrap()
}

fn matic() -> AccountId {
    ACCOUNT_ID_NETWORK_FUNGIBLE_FAUCET.try_into().unwrap()
}
