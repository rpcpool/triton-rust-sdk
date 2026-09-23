#![allow(clippy::large_enum_variant)]

pub mod account_sync {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    pub use crate::geyser;

    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

pub mod geyser {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/geyser.rs"));
}

pub mod solana {
    #![allow(clippy::missing_const_for_fn)]

    pub mod storage {
        pub mod confirmed_block {
            include!(concat!(
                env!("OUT_DIR"),
                "/solana.storage.confirmed_block.rs"
            ));
        }
    }
}

use yellowstone_grpc_proto::geyser::CommitmentLevel as YellowstoneCommitmentLevel;

impl From<geyser::CommitmentLevel> for YellowstoneCommitmentLevel {
    fn from(value: geyser::CommitmentLevel) -> Self {
        match value {
            geyser::CommitmentLevel::Processed => Self::Processed,
            geyser::CommitmentLevel::Confirmed => Self::Confirmed,
            geyser::CommitmentLevel::Finalized => Self::Finalized,
        }
    }
}
