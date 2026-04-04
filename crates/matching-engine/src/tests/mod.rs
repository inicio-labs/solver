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
use miden_protocol::note::NoteId;
use miden_protocol::Felt;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_3,
    ACCOUNT_ID_NETWORK_FUNGIBLE_FAUCET,
};

/// Create a deterministic NoteId from a u64 seed.
fn make_note_id(seed: u64) -> NoteId {
    let w1 = [Felt::new(seed), Felt::new(seed + 1), Felt::new(seed + 2), Felt::new(seed + 3)];
    let w2 = [Felt::new(seed + 4), Felt::new(seed + 5), Felt::new(seed + 6), Felt::new(seed + 7)];
    NoteId::new(w1.into(), w2.into())
}

/// Sequential NoteId counter for tests. Returns a unique NoteId each call.
struct NoteIdGen(u64);
impl NoteIdGen {
    fn new() -> Self { Self(1000) }
    fn next(&mut self) -> NoteId {
        let id = make_note_id(self.0);
        self.0 += 100;
        id
    }
}

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
