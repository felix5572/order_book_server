//! Account-level liquidation cooldown evidence (design 000443 r2 revision 2, r3 revision 3): after a partial market
//! liquidation, the account's market liquidations take whole positions for 30 s (official "Liquidations"; research
//! `hl_liquidation_rules_20261020`: 122 of 122 market liquidations within it were whole).
//!
//! Evidence comes from completed blocks: only the liquidated account's own fills (`liquidation.liquidatedUser` is the
//! fill's account). One block's market liquidation fills of an account in one coin are one event; it is partial when
//! they took less than the position they started from. A liquidation the view cannot size (a coin outside the anchor's
//! universe, a size that does not read) leaves the account unknown for the cooldown's length. That none happened needs
//! the fills stream applied without a loss over the last 30 s of block time; an anchor does not restore that history.

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use crate::types::{Fill, node_data::NodeDataFill};

pub(crate) const COOLDOWN_MS: u64 = 30_000;
const MARKET_METHOD: &str = "market";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cooldown {
    /// A partial market liquidation within the last 30 s of block time.
    Active,
    /// None in the last 30 s, every block of them applied without a fills loss.
    None,
    Unknown,
}

/// One block's market liquidation of an account in a coin: the position before its first fill and the size taken, or
/// unsized.
type Event = Result<(i64, i64), ()>;

#[derive(Debug, Default)]
pub(crate) struct LiquidationHistory {
    /// Block time of each account's latest partial market liquidation.
    last_partial_ms: FxHashMap<Address, u64>,
    /// Block time of each account's latest market liquidation the view could not size.
    last_unsized_ms: FxHashMap<Address, u64>,
    /// Block time from which every block's fills were applied without a loss; `None` before the first block and from a
    /// loss to the block after the one it was noticed at.
    covered_since_ms: Option<u64>,
    /// The fills stream's loss markers last seen.
    losses_seen: (Option<u64>, Option<u64>),
}

impl LiquidationHistory {
    /// A block of time `time_ms` is being applied; `losses` are the fills stream's loss markers (highest lost block,
    /// latest unknown loss) as they stand before its fills. A changed marker restarts the coverage after this block.
    pub(crate) fn begin_block(&mut self, time_ms: u64, losses: (Option<u64>, Option<u64>)) {
        if losses != self.losses_seen {
            self.losses_seen = losses;
            self.covered_since_ms = None;
            return;
        }
        self.covered_since_ms.get_or_insert(time_ms);
    }

    /// The market liquidations among one completed block's fills, every line of it at once (r3 revision 3: one event per
    /// block, account and coin); `size_fill` reads a fill's position before it and the size it took, raw
    /// (10^-szDecimals), or fails (the coin outside the universe included).
    pub(crate) fn record_block<'f>(
        &mut self,
        time_ms: u64,
        fills: impl IntoIterator<Item = &'f NodeDataFill>,
        size_fill: impl Fn(&Fill) -> Result<(i64, i64), String>,
    ) {
        let mut events: FxHashMap<(Address, &str), Event> = FxHashMap::default();
        for NodeDataFill(user, fill) in fills {
            let Some(liquidation) = fill.liquidation.as_ref().filter(|liquidation| liquidation.method == MARKET_METHOD) else {
                continue;
            };
            // an address that does not read cannot tell the account's own fill from its counterparty's: unsized
            let liquidated = liquidation.liquidated_user.parse::<Address>();
            if liquidated.as_ref().is_ok_and(|liquidated| liquidated != user) {
                continue;
            }
            let sized = liquidated.map_err(|_| ()).and_then(|_| size_fill(fill).map_err(|_| ()));
            events
                .entry((*user, fill.coin.as_str()))
                .and_modify(|event| {
                    *event = match (*event, sized) {
                        (Ok((start, taken)), Ok((_, more))) => Ok((start, taken + more)),
                        _ => Err(()),
                    };
                })
                .or_insert(sized);
        }
        for ((user, _), event) in events {
            match event {
                Ok((start, taken)) if taken < start.abs() => {
                    self.last_partial_ms.insert(user, time_ms);
                }
                Ok(_) => {}
                Err(()) => {
                    self.last_unsized_ms.insert(user, time_ms);
                }
            }
        }
    }

    /// The account's cooldown at block time `now_ms`.
    pub(crate) fn state(&self, user: &Address, now_ms: u64) -> Cooldown {
        let recent = |at: Option<&u64>| at.is_some_and(|at| now_ms.saturating_sub(*at) < COOLDOWN_MS);
        if recent(self.last_partial_ms.get(user)) {
            return Cooldown::Active;
        }
        let covered = self.covered_since_ms.is_some_and(|since| now_ms.saturating_sub(since) >= COOLDOWN_MS);
        if !covered || recent(self.last_unsized_ms.get(user)) {
            return Cooldown::Unknown;
        }
        Cooldown::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Liquidation;

    const LIQUIDATED: Address = Address::repeat_byte(0xee);
    const T: u64 = 1_792_391_844_000;

    fn liquidation_fill(user: Address, liquidated: Address, start: &str, sz: &str, method: &str) -> NodeDataFill {
        let wire = serde_json::json!({"coin": "PUMP", "px": "0.004", "sz": sz, "side": "A", "time": 1, "startPosition": start,
            "dir": "Close Long", "closedPnl": "0.0", "hash": "0x0", "oid": 1, "crossed": true, "fee": "0.0", "tid": 1,
            "feeToken": "USDC"});
        let mut fill: Fill = serde_json::from_value(wire).unwrap();
        fill.liquidation =
            Some(Liquidation { liquidated_user: liquidated.to_string(), mark_px: "0.004".to_string(), method: method.to_string() });
        NodeDataFill(user, fill)
    }

    fn sizes(fill: &Fill) -> Result<(i64, i64), String> {
        Ok((fill.start_position.parse::<i64>().map_err(|err| err.to_string())?, fill.sz.parse::<i64>().map_err(|err| err.to_string())?))
    }

    /// Covered for 30 s from `T - 30 s` on.
    fn covered() -> LiquidationHistory {
        let mut history = LiquidationHistory::default();
        history.begin_block(T - COOLDOWN_MS, (None, None));
        history
    }

    /// Design 000443 r2 revision 2: a fifth taken in two fills of one block is partial; the whole is not; the
    /// counterparty's fill and a backstop are not the account's market liquidation.
    #[test]
    fn a_partial_market_liquidation_starts_a_cooldown_of_the_account() {
        let mut history = covered();
        let other = Address::repeat_byte(1);
        let fills = [
            liquidation_fill(LIQUIDATED, LIQUIDATED, "1000", "100", "market"),
            liquidation_fill(LIQUIDATED, LIQUIDATED, "900", "100", "market"),
            liquidation_fill(other, LIQUIDATED, "-50", "200", "market"),
            liquidation_fill(other, other, "500", "500", "market"),
            liquidation_fill(Address::repeat_byte(2), Address::repeat_byte(2), "500", "100", "backstop"),
        ];
        history.record_block(T, &fills, sizes);
        assert_eq!(history.state(&LIQUIDATED, T + COOLDOWN_MS - 1), Cooldown::Active, "200 of 1000 taken");
        assert_eq!(history.state(&LIQUIDATED, T + COOLDOWN_MS), Cooldown::None, "expired, every block covered");
        assert_eq!(history.state(&other, T), Cooldown::None, "a whole position is no cooldown");
        assert_eq!(history.state(&Address::repeat_byte(2), T), Cooldown::None, "a backstop is not a market liquidation");
    }

    /// Design 000443 r3 revision 3: none needs 30 s of covered block time; a loss restarts it after the block it was
    /// noticed at, and an unsized liquidation is unknown for the cooldown's length.
    #[test]
    fn none_needs_thirty_covered_seconds() {
        let mut history = LiquidationHistory::default();
        assert_eq!(history.state(&LIQUIDATED, T), Cooldown::Unknown, "nothing applied yet");
        history.begin_block(T, (None, None));
        assert_eq!(history.state(&LIQUIDATED, T + COOLDOWN_MS - 1), Cooldown::Unknown);
        assert_eq!(history.state(&LIQUIDATED, T + COOLDOWN_MS), Cooldown::None);
        history.begin_block(T + 40_000, (Some(5), None));
        assert_eq!(history.state(&LIQUIDATED, T + 40_000), Cooldown::Unknown, "a loss noticed");
        history.begin_block(T + 41_000, (Some(5), None));
        assert_eq!(history.state(&LIQUIDATED, T + 41_000 + COOLDOWN_MS), Cooldown::None, "covered from the next block");
        let not_sized = |_: &Fill| Err("PUMP is outside the universe".to_string());
        history.record_block(T + 80_000, &[liquidation_fill(LIQUIDATED, LIQUIDATED, "1000", "1000", "market")], not_sized);
        assert_eq!(history.state(&LIQUIDATED, T + 80_000), Cooldown::Unknown);
        assert_eq!(history.state(&LIQUIDATED, T + 80_000 + COOLDOWN_MS), Cooldown::None);
    }

    /// Design 000443 r3: a cooldown ending does not make it none while the last 30 s are not covered.
    #[test]
    fn an_expired_cooldown_without_coverage_is_unknown() {
        let mut history = covered();
        history.record_block(T, &[liquidation_fill(LIQUIDATED, LIQUIDATED, "1000", "200", "market")], sizes);
        history.begin_block(T + 10_000, (None, Some(T + 10_000)));
        assert_eq!(history.state(&LIQUIDATED, T + COOLDOWN_MS), Cooldown::Unknown);
    }
}
