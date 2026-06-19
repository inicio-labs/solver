use miden_core::Felt;
use miden_crypto::hash::poseidon2::Poseidon2;
use miden_protocol::asset::Asset;
use miden_protocol::transaction::TransactionScript;
use miden_protocol::utils::sync::LazyLock;
use miden_protocol::Word;
use miden_standards::code_builder::CodeBuilder;

/// MASM source for the consume-asset transaction script.
const CONSUME_ASSET_MASM: &str = include_str!("../asm/consume_asset.masm");

/// Compiled consume-asset-script, assembled at first use.
static CONSUME_ASSET_SCRIPT: LazyLock<TransactionScript> = LazyLock::new(|| {
    let code_builder = CodeBuilder::new();
    code_builder
        .compile_tx_script(CONSUME_ASSET_MASM)
        .expect("failed to compile consume-asset script")
});

/// Output of [`ConsumeAssetScript::prepare`]: everything needed to wire
/// the consume-asset-script into a transaction context.
pub struct ConsumeAssetData {
    /// The commitment word passed as the tx-script argument.
    pub commitment_arg: Word,
    /// `(key, value)` pair to feed into `extend_advice_map`.
    pub advice_map_entry: (Word, Vec<Felt>),
}

/// SDK helper for the consume-asset-script: consumes surplus/spread assets
/// directly into the executing account's vault.
///
/// # Usage
///
/// ```ignore
/// let tx_script = ConsumeAssetScript::tx_script();
/// let data = ConsumeAssetScript::prepare(&[surplus_asset]);
/// // Wire tx_script, data.commitment_arg, data.advice_map_entry into transaction
/// ```
pub struct ConsumeAssetScript;

impl ConsumeAssetScript {
    /// Returns the compiled `TransactionScript` for the consume-asset-script.
    pub fn tx_script() -> TransactionScript {
        CONSUME_ASSET_SCRIPT.clone()
    }

    /// Prepares the commitment and advice map for consuming assets
    /// into the executing account's vault.
    ///
    /// The advice map entry contains: `[num_assets, asset_key_0..., asset_value_0..., ...]`
    /// The commitment is a Poseidon2 hash of this data, reversed for stack ordering.
    pub fn prepare(assets: &[Asset]) -> ConsumeAssetData {
        assert!(!assets.is_empty(), "must provide at least one asset");

        let num_assets = assets.len() as u64;
        let mut advice_felts: Vec<Felt> = Vec::with_capacity(1 + assets.len() * 8);

        // First element: number of assets (read by the MASM script via adv_push.1).
        // `Felt::new` is fallible since miden-core 0.23; an asset count trivially
        // fits in the field, so this never errors.
        advice_felts.push(Felt::new(num_assets).expect("asset count fits in the field"));

        for asset in assets {
            let key_word = asset.to_key_word();
            let value_word = asset.to_value_word();
            advice_felts.extend(key_word);
            advice_felts.extend(value_word);
        }

        // Compute Poseidon2 commitment over all advice felts
        let commitment_key: Word = Poseidon2::hash_elements(&advice_felts);
        let mut commitment_arg = commitment_key;
        commitment_arg.reverse();

        ConsumeAssetData {
            commitment_arg,
            advice_map_entry: (commitment_key, advice_felts),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_compiles_successfully() {
        let _script = ConsumeAssetScript::tx_script();
    }
}
