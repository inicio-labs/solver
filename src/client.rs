use std::sync::Arc;

use anyhow::{Context, Result};
use miden_client::{
    keystore::FilesystemKeyStore,
    note::Note,
    rpc::{Endpoint, GrpcClient, NodeRpcClient},
    store::NoteFilter,
    builder::ClientBuilder,
    Client,
};
use miden_client_sqlite_store::ClientBuilderSqliteExt;
use miden_protocol::account::AccountId;
use miden_protocol::asset::{Asset, FungibleAsset};
use miden_protocol::note::NoteType;
use miden_swapp::PswapNote;

use crate::config::SolverConfig;

pub struct SolverClient {
    pub client: Client<FilesystemKeyStore>,
}

impl SolverClient {
    pub async fn new(config: &SolverConfig) -> Result<Self> {
        let endpoint = Endpoint::try_from(config.rpc.endpoint.as_str())
            .map_err(|e| anyhow::anyhow!("Failed to parse endpoint: {}", e))?;
        let timeout_ms = config.rpc.timeout_ms;
        let rpc_client: Arc<dyn NodeRpcClient> =
            Arc::new(GrpcClient::new(&endpoint, timeout_ms));

        let keystore_path = std::path::PathBuf::from(&config.solver.keystore_path);
        let keystore = Arc::new(
            FilesystemKeyStore::new(keystore_path)
                .context("Failed to initialize keystore")?,
        );

        let store_path = std::path::PathBuf::from(&config.solver.store_path);

        let client = ClientBuilder::new()
            .rpc(rpc_client)
            .sqlite_store(store_path)
            .authenticator(keystore)
            .in_debug_mode(true.into())
            .build()
            .await
            .context("Failed to build Miden client")?;

        Ok(Self { client })
    }

    /// Register PSWAP note tags for an asset pair so that `sync_state` fetches
    /// notes for both trade directions (offers X wants Y, and offers Y wants X).
    pub async fn register_pair_tags(
        &mut self,
        faucet_x: AccountId,
        faucet_y: AccountId,
    ) -> Result<()> {
        let asset_x = Asset::Fungible(FungibleAsset::new(faucet_x, 1)
            .map_err(|e| anyhow::anyhow!("Failed to create asset X: {}", e))?);
        let asset_y = Asset::Fungible(FungibleAsset::new(faucet_y, 1)
            .map_err(|e| anyhow::anyhow!("Failed to create asset Y: {}", e))?);

        let tag_xy = PswapNote::build_tag(NoteType::Public, &asset_x, &asset_y);
        let tag_yx = PswapNote::build_tag(NoteType::Public, &asset_y, &asset_x);

        self.client.add_note_tag(tag_xy).await
            .context("Failed to register tag for X->Y")?;
        self.client.add_note_tag(tag_yx).await
            .context("Failed to register tag for Y->X")?;

        Ok(())
    }

    pub async fn fetch_pswap_notes(&mut self) -> Result<Vec<Note>> {
        self.client.sync_state().await?;

        let note_records = self.client.get_input_notes(NoteFilter::Committed).await?;

        let notes: Vec<Note> = note_records
            .into_iter()
            .filter_map(|record| {
                let note: Result<Note, _> = record.try_into();
                note.ok()
            })
            .collect();

        Ok(notes)
    }
}
