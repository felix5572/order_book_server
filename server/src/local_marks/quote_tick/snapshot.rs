//! What the external quote tick publishes (hyperliquid-trade design 000673 r3 §2, r4 首帧): each subscribed coin's
//! latest frame on each channel, serialized once, with the version of the tick that published it - the tick's own
//! count, rising - and the subscription epoch it was evaluated in (`subscribers`). A coin without a subscriber left has
//! no frame. The last snapshot of a withdrawn tick names why and holds no frame.

use bytes::Bytes;
use rustc_hash::FxHashMap;
use tokio::time::Instant;

use crate::types::subscription::{EXT_EXTERNAL_QUOTES, EXT_HIP3_EXTERNAL_QUOTES};

/// An external quote channel: `extExternalQuotes`, the main-dex coins'; `extHip3ExternalQuotes`, the HIP-3 coins'.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum QuoteChannel {
    Main,
    Hip3,
}

impl QuoteChannel {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Main => EXT_EXTERNAL_QUOTES,
            Self::Hip3 => EXT_HIP3_EXTERNAL_QUOTES,
        }
    }
}

/// A coin of a channel, by its place in the channel's table: the main-dex coins' (`CoinTable`), the HIP-3 external
/// coins' (`Hip3ExternalTable`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct QuoteKey {
    pub(crate) channel: QuoteChannel,
    pub(crate) index: usize,
}

/// A coin's latest frame: `version`, the tick's that published it; `epoch`, the subscription epoch the tick read
/// before evaluating it; `sample`, its timing's start when the tick sampled it (`publish::SAMPLE_INTERVAL`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuoteEntry {
    pub(crate) version: u64,
    pub(crate) epoch: u32,
    pub(crate) frame: Bytes,
    pub(crate) sample: Option<QuoteSample>,
}

/// A change frame sampled for the timing of its way out (review 000676 r1 P2): the newest receipt of the quotes whose
/// move published it, and its publish time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuoteSample {
    /// This server's wall clock, ms: the slot's `received_ms` of the newest quote of the legs whose quote moved.
    pub(crate) received_ms: u64,
    /// The frame's `publishedTsMs`, and the same moment on the monotonic clock.
    pub(crate) published_ms: u64,
    pub(crate) published_at: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct QuoteSnapshot {
    entries: FxHashMap<QuoteKey, QuoteEntry>,
    /// Why both channels are withdrawn until restart; set on the tick's last snapshot only.
    withdrawn: Option<String>,
}

impl QuoteSnapshot {
    pub(crate) fn of(entries: impl IntoIterator<Item = (QuoteKey, QuoteEntry)>) -> Self {
        Self { entries: entries.into_iter().collect(), withdrawn: None }
    }

    /// The tick's last: both channels withdrawn for `reason`.
    pub(crate) fn withdrawn_for(reason: String) -> Self {
        Self { entries: FxHashMap::default(), withdrawn: Some(reason) }
    }

    /// The coin's latest frame; `None` while it has no subscriber, or before the first tick that read one.
    pub(crate) fn entry(&self, key: QuoteKey) -> Option<&QuoteEntry> {
        self.entries.get(&key)
    }

    pub(crate) fn withdrawn(&self) -> Option<&str> {
        self.withdrawn.as_deref()
    }

    /// How many coins have a frame.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}
