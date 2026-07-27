// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_telemetry::InstrumentProvider;

#[derive(Clone)]
pub struct LoreStorageService {
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    local_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
}

impl LoreStorageService {
    pub fn new(
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        local_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
    ) -> Self {
        Self {
            immutable_store,
            local_store,
            mutable_store,
        }
    }

    pub fn local_immutable_store(&self) -> &Arc<dyn lore_storage::ImmutableStore> {
        &self.local_store
    }

    pub fn immutable_store(&self) -> &Arc<dyn lore_storage::ImmutableStore> {
        &self.immutable_store
    }

    pub fn mutable_store(&self) -> &Arc<dyn lore_storage::MutableStore> {
        &self.mutable_store
    }
}

impl InstrumentProvider for LoreStorageService {
    fn namespace(&self) -> &'static str {
        "urc.grpc.storage_service"
    }
}
