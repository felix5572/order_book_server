//! Main-dex automatic refresh from the last submitted prices. Native a6e2897c and SAND
//! 1197480000..1197490000: the one-second begin-block guard precedes the stale deadline
//! and three-second refresh guard. An automatic write does not advance the explicit-SGA clock.

use serde::Deserialize;
use crate::accounts::checkpoint::node_time_ns;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub(crate) struct BucketGuard {
    last_bucket: u64,
    bucket_millis: u64,
}

impl BucketGuard {
    fn validate(&self) -> Result<(), String> {
        if self.bucket_millis == 0 { return Err("zero automatic-price bucket duration".into()); }
        Ok(())
    }

    fn advance(&mut self, time_ns: u64) -> bool {
        let next = time_ns / 1_000_000 / self.bucket_millis;
        if next <= self.last_bucket { return false; }
        self.last_bucket = next;
        true
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct WireRefreshConfig {
    // A missing field is not the explicit null which disables refresh.
    #[serde(deserialize_with = "Deserialize::deserialize")]
    sgd: Option<f64>,
    sog: BucketGuard,
    mou: WireSubmittedPrices,
}

#[derive(Debug, Deserialize)]
struct WireSubmittedPrices {
    // Oracle, mark, external-perp inputs. Only the first two feed the main selector.
    u: [Vec<(usize, u64)>; 3],
}

/// Missing archive sections withhold marks while account balances can still be loaded.
#[derive(Debug)]
pub(crate) struct WireRefreshSeed {
    pub(crate) config: Option<WireRefreshConfig>,
    pub(crate) outer: Option<BucketGuard>,
    pub(crate) post_only_until_time: Option<String>,
    pub(crate) post_only_until_height: Option<u64>,
    pub(crate) hardfork_height: Option<u64>,
    pub(crate) last_explicit_time: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefreshState {
    outer: BucketGuard,
    inner: BucketGuard,
    stale_after_ns: Option<u64>,
    last_explicit_ns: u64,
    post_only_until_ns: u64,
    post_only_until_height: u64,
    hardfork_height: u64,
    pub(crate) submitted: Vec<Option<(u64, u64)>>,
}

impl RefreshState {
    /// The state, and the assets whose cached inputs hold one of the oracle and the mark (bm 10-27 XTEN at
    /// 1198840000: the mark alone): unsupported coins, their submitted prices unknown.
    pub(crate) fn from_wire(wire: WireRefreshSeed, count: usize) -> Result<(Self, Vec<usize>), String> {
        let config = wire.config.ok_or("missing exchange.msc automatic-price state")?;
        let outer = wire.outer.ok_or("missing begin_block_logic_guard")?;
        outer.validate()?;
        config.sog.validate()?;
        let stale_after_ns = config.sgd.map(|seconds| {
            let ns = seconds * 1e9;
            if !ns.is_finite() || ns < 0.0 || ns >= u64::MAX as f64 || ns.fract() != 0.0 {
                return Err("sgd is not a nonnegative integral nanosecond duration".to_string());
            }
            Ok(ns as u64)
        }).transpose()?;
        let [oracle, mark, _external] = config.mou.u;
        let oracle = input_by_asset(oracle, count)?;
        let mark = input_by_asset(mark, count)?;
        let mut partial = Vec::new();
        let submitted = oracle.into_iter().zip(mark).enumerate().map(|(asset, pair)| match pair {
            (Some(o), Some(m)) => Some((o, m)),
            (None, None) => None,
            _ => {
                partial.push(asset);
                None
            }
        }).collect();
        let state = Self {
            outer, inner: config.sog, stale_after_ns,
            last_explicit_ns: node_time_ns(&wire.last_explicit_time.ok_or("missing main lnsgt")?)?,
            post_only_until_ns: node_time_ns(&wire.post_only_until_time.ok_or("missing post_only_until_time")?)?,
            post_only_until_height: wire.post_only_until_height.ok_or("missing post_only_until_height")?,
            hardfork_height: wire.hardfork_height.ok_or("missing ctx.hardfork_height")?,
            submitted,
        };
        Ok((state, partial))
    }

    pub(crate) fn due(&mut self, height: u64, time_ns: u64) -> bool {
        if !self.outer.advance(time_ns) { return false; }
        let Some(stale_after_ns) = self.stale_after_ns else { return false };
        let started = time_ns >= self.post_only_until_ns && height >= self.post_only_until_height;
        if !started && height < self.hardfork_height.saturating_add(5_000) { return false; }
        let Some(deadline) = self.last_explicit_ns.checked_add(stale_after_ns) else { return false };
        time_ns >= deadline && self.inner.advance(time_ns)
    }

    /// The seed's assets an explicit SGA prices otherwise than their submitted prices (`coverage`: priced, `None` for
    /// one of the two). The retained SAND evidence covers unchanged publisher coverage: the native cache of an input
    /// added or dropped is not established, so such an asset's automatic refresh is unknown until the next checkpoint
    /// seeds it; each asset refreshes from its own submitted prices alone, so the others are not affected. An asset
    /// listed since the seed has no state to refresh.
    pub(crate) fn coverage_changes(&self, coverage: impl Iterator<Item = Option<bool>>) -> Vec<usize> {
        if self.stale_after_ns.is_none() { return Vec::new(); }
        let mut coverage = coverage;
        let changed = self.submitted.iter().enumerate().filter_map(|(asset, submitted)| {
            let priced = coverage.next().unwrap_or(Some(false))?;
            (priced != submitted.is_some()).then_some(asset)
        });
        changed.collect()
    }

    pub(crate) fn explicit(&mut self, time_ns: u64) {
        self.last_explicit_ns = time_ns;
    }
}

fn input_by_asset(values: Vec<(usize, u64)>, count: usize) -> Result<Vec<Option<u64>>, String> {
    let mut out = vec![None; count];
    for (asset, value) in values {
        let slot = out.get_mut(asset).ok_or_else(|| format!("cached price asset {asset} outside universe"))?;
        if value == 0 || slot.replace(value).is_some() { return Err(format!("invalid or duplicate cached price asset {asset}")); }
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod sand_tests;
