//! Runtime diagnostics, separate from input validity and business state. Suppression only limits log I/O:
//! every observation still updates the existing metrics and the account/mark owners' loss bounds.
use std::time::{Duration, Instant};
use rustc_hash::FxHashMap;
use super::book::Stream;
use crate::metrics::ACCOUNTS_DIAGNOSTIC_EVENTS_TOTAL;

const SUMMARY_INTERVAL: Duration = Duration::from_secs(30);
const DETAIL_BYTES: usize = 1024;
const RESEARCH_INTERVAL: Duration = Duration::from_secs(600);
const RESEARCH_SAMPLES: u64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Cause {
    Positioning, WatcherDiscard, LateOrDropped, Parse, HeightGap, PendingOverflow,
    Reconstruction, SizeReconstruction, Merge, HeldOverflow, LogOverflow, FundingDependency,
}
impl Cause {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Positioning => "positioning", Self::WatcherDiscard => "watcher_discard",
            Self::LateOrDropped => "late_or_dropped", Self::Parse => "parse_error", Self::HeightGap => "height_gap",
            Self::PendingOverflow => "pending_overflow", Self::Reconstruction => "reconstruction",
            Self::SizeReconstruction => "size_reconstruction", Self::Merge => "anchor_merge",
            Self::HeldOverflow => "hold_overflow", Self::LogOverflow => "replay_log_overflow",
            Self::FundingDependency => "funding_dependency",
        }
    }
    pub(super) const fn category(self) -> &'static str {
        match self {
            Self::Positioning => "initialization",
            Self::WatcherDiscard | Self::LateOrDropped | Self::HeightGap => "inputContinuity",
            Self::Parse => "inputFormat",
            Self::PendingOverflow | Self::HeldOverflow | Self::LogOverflow => "capacity",
            Self::Reconstruction | Self::SizeReconstruction | Self::Merge => "reconstruction",
            Self::FundingDependency => "missingDependency",
        }
    }
    const fn level(self) -> log::Level {
        match self {
            Self::Positioning => log::Level::Info,
            Self::Parse | Self::PendingOverflow | Self::HeldOverflow | Self::LogOverflow => log::Level::Error,
            _ => log::Level::Warn,
        }
    }
}

#[derive(Debug)]
struct Episode {
    total: u64,
    pending: u64,
    first_block: Option<u64>,
    highest_block: Option<u64>,
    first_detail: String,
    last_report: Instant,
}
impl Episode {
    fn observe(&mut self, block: Option<u64>) {
        self.total += 1;
        self.pending += 1;
        if let Some(block) = block {
            self.first_block = Some(self.first_block.map_or(block, |old| old.min(block)));
            self.highest_block = Some(self.highest_block.map_or(block, |old| old.max(block)));
        }
    }
}

/// Fixed cause/stream keys bound memory independently of account, coin, error text and block count.
#[derive(Default)]
pub(super) struct AccountDiagnostics {
    episodes: FxHashMap<(Option<Stream>, Cause), Episode>,
    research: FxHashMap<Stream, (Instant, u64)>,
}
impl AccountDiagnostics {
    pub(super) fn report(&mut self, stream: Option<Stream>, cause: Cause, block: Option<u64>, detail: impl FnOnce() -> String) {
        let first = self.observe(stream, cause, block, Instant::now(), detail);
        ACCOUNTS_DIAGNOSTIC_EVENTS_TOTAL.with_label_values(&[cause.category(), cause.label(), if first { "sampled" } else { "aggregated" }]).inc();
        if first {
            let episode = &self.episodes[&(stream, cause)];
            log::log!(cause.level(), "accounts diagnostic category={} cause={} stream={} block={block:?}: {}",
                cause.category(), cause.label(), stream.map_or("account_view", Stream::label), episode.first_detail);
        }
    }

    fn observe(&mut self, stream: Option<Stream>, cause: Cause, block: Option<u64>, now: Instant, detail: impl FnOnce() -> String) -> bool {
        if let Some(episode) = self.episodes.get_mut(&(stream, cause)) {
            episode.observe(block);
            return false;
        }
        let text = detail();
        let mut end = text.len().min(DETAIL_BYTES);
        while !text.is_char_boundary(end) { end -= 1; }
        let first_detail = if end < text.len() {
            format!("{} [truncated; originalBytes={}]", &text[..end], text.len())
        } else { text };
        self.episodes.insert((stream, cause), Episode {
            total: 1, pending: 0, first_block: block, highest_block: block, first_detail, last_report: now,
        });
        true
    }

    /// The owner's supervision tick writes one summary per key. Recovery means both views are servable again;
    /// it does not claim that the upstream cause is fixed. A new observation after recovery is sampled immediately.
    pub(super) fn tick(&mut self, now: Instant, recovered_through: Option<u64>) {
        for ((stream, cause), episode) in &mut self.episodes {
            let views_recovered = recovered_through.is_some_and(|height| episode.highest_block.is_none_or(|block| height >= block));
            if !views_recovered && (episode.pending == 0 || now.duration_since(episode.last_report) < SUMMARY_INTERVAL) { continue; }
            let phase = if views_recovered { "viewsRebuilt" } else { "ongoing" };
            log::log!(if views_recovered { log::Level::Info } else { cause.level() },
                "accounts diagnostic summary phase={phase} category={} cause={} stream={} total={} aggregatedSinceLast={} blocks={:?}..{:?}",
                cause.category(), cause.label(), stream.map_or("account_view", Stream::label), episode.total,
                episode.pending, episode.first_block, episode.highest_block);
            episode.pending = 0;
            episode.last_report = now;
        }
        if let Some(height) = recovered_through {
            self.episodes.retain(|_, episode| episode.highest_block.is_some_and(|block| block > height));
        }
    }

    /// Research selection is not a protocol error. Its quota follows elapsed time, not a seed or reconnect.
    pub(super) fn sample_research(&mut self, stream: Stream, now: Instant) -> bool {
        let (start, seen) = self.research.entry(stream).or_insert((now, 0));
        if now.duration_since(*start) >= RESEARCH_INTERVAL { *start = now; *seen = 0; }
        *seen += 1;
        let sample = *seen <= RESEARCH_SAMPLES;
        ACCOUNTS_DIAGNOSTIC_EVENTS_TOTAL.with_label_values(&["research", "rare_line", if sample { "sampled" } else { "aggregated" }]).inc();
        sample
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_replay_flood_keeps_one_bounded_example_and_all_counts_until_recovery() {
        let now = Instant::now();
        let mut d = AccountDiagnostics::default();
        assert!(d.observe(Some(Stream::Fills), Cause::LateOrDropped, Some(120), now, || "测".repeat(10_000)));
        for block in 0..100_000 {
            assert!(!d.observe(Some(Stream::Fills), Cause::LateOrDropped, Some(block), now, || panic!("suppressed details are not formatted")));
        }
        let e = &d.episodes[&(Some(Stream::Fills), Cause::LateOrDropped)];
        assert_eq!((e.total, e.pending, e.first_block, e.highest_block), (100_001, 100_000, Some(0), Some(99_999)));
        assert!(e.first_detail.len() < 1100);
        d.tick(now + SUMMARY_INTERVAL, None);
        assert_eq!(d.episodes[&(Some(Stream::Fills), Cause::LateOrDropped)].pending, 0);
        d.tick(now + SUMMARY_INTERVAL, Some(99_999));
        assert!(d.observe(Some(Stream::Fills), Cause::LateOrDropped, Some(100_001), now + SUMMARY_INTERVAL, || "new incident".into()));
    }
    #[test]
    fn distinct_causes_and_streams_get_their_own_first_evidence() {
        let mut d = AccountDiagnostics::default();let now = Instant::now();
        for (stream, cause) in [(Stream::Fills, Cause::Positioning), (Stream::Fills, Cause::Parse), (Stream::BookDiffs, Cause::Parse)] {
            assert!(d.observe(Some(stream), cause, None, now, || "context".into()));
        }
        assert_eq!(d.episodes.len(), 3);
        assert_eq!(Cause::Positioning.level(), log::Level::Info);
        assert_eq!(Cause::Reconstruction.category(), "reconstruction");
        assert_eq!(Cause::LateOrDropped.category(), "inputContinuity");
    }
    #[test]
    fn an_available_view_with_an_old_anchor_does_not_clear_funding_dependency() {
        let now = Instant::now();let mut d = AccountDiagnostics::default();
        d.observe(Some(Stream::MiscEvents), Cause::FundingDependency, Some(200), now, || "missing oracle".into());
        d.tick(now, Some(199));
        assert_eq!(d.episodes.len(), 1);
        d.tick(now, Some(200));
        assert!(d.episodes.is_empty());
    }
    #[test]
    fn research_samples_are_bounded_and_recovery_does_not_reset_the_quota() {
        let mut d = AccountDiagnostics::default();let now = Instant::now();
        for i in 0..1000 { assert_eq!(d.sample_research(Stream::Fills, now), i < RESEARCH_SAMPLES); }
        assert!(d.sample_research(Stream::MiscEvents, now));
        d.tick(now, Some(0));
        assert!(!d.sample_research(Stream::Fills, now));
        assert!(d.sample_research(Stream::Fills, now + RESEARCH_INTERVAL));
    }
}
