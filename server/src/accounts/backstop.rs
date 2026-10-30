//! Backstop liquidations (hyperliquid-trade research `backstop_liquidation_20261029`, bm 2026-10-29 20:01 UTC, the
//! Intel/Amazon earnings: 706 HIP-3 accounts in an hour). The liquidator takes every position of one pool (an
//! isolated position, or a dex's cross positions) over in one block. Its misc events carry the pool's `liquidation`
//! ledger update ([`TakeoverRecord`]); its fills one pair per position, the liquidated account's side and the
//! liquidator's, with the fill's `markPx` and no fee, at
//!
//! `px = mark × (1 − sign × accountValue / liquidatedNtlPos)`, `sign` the position's,
//!
//! rounded to the price unit (67 of 67 records: 20 isolated from the OB's evidence log, 47 from the official API's
//! ledger, 38 cross among them and two of those with two positions; every long rounded down, the liquidator's way,
//! within 0.043 bp). At those prices the pool is left nothing but the rounding, so the liquidated side is applied as an
//! ordinary fill of its pool. Not measured: where the rounding's remainder goes, a unified account's cross pool, the
//! liquidator's own cash.
//!
//! The block's evidence travels with each fill event ([`TakeoverEvidence`]); the cash replay checks it against its
//! anchor's universe when it applies the fill, so an anchor installed after the block reads it the same.

use std::sync::Arc;

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::{
    checkpoint::MAX_PERP_DECIMALS,
    misc::{LedgerDelta, MiscEvent},
    size::parse_size_raw,
    usdc::WEI_PER_MICRO,
};
use crate::{
    order_book::Side,
    types::{Fill, node_data::NodeDataFill},
};

/// The direction of an ADL fill: it carries the `backstop` method but is an ordinary fill.
pub(crate) const ADL_DIR: &str = "Auto-Deleveraging";
const BACKSTOP_METHOD: &str = "backstop";

/// The pool a takeover empties (`leverageType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TakeoverPool {
    Cross,
    Isolated,
}

/// A `liquidation` ledger update: the pool's value and notional at mark when the liquidator took it over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TakeoverRecord {
    pub(crate) pool: TakeoverPool,
    /// `accountValue`, 10^-8 USDC.
    pub(crate) value_wei: i64,
    /// `liquidatedNtlPos`, 10^-8 USDC.
    pub(crate) notional_wei: i64,
    /// `liquidatedPositions`: each coin with its signed size, as written.
    pub(crate) positions: Vec<(String, String)>,
}

/// A liquidated side's backstop fill, as the rule reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TakenFill {
    coin: String,
    side: Side,
    px: String,
    sz: String,
    start_position: String,
    mark_px: String,
}

/// A liquidated account's evidence of one block: the `liquidation` records naming it and its backstop fills. It goes
/// with each of those fill events into the event log, so the rule is checked against the universe of whichever anchor
/// replays them (an anchor may install after the block).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TakeoverEvidence {
    records: Vec<TakeoverRecord>,
    fills: Vec<TakenFill>,
}

impl TakeoverEvidence {
    /// The pool the block's one record takes over, each listed position by one of the fills, whole, at the rule's
    /// price, their notional at mark the record's; or why not. `size_decimals`: a perp's, `None` outside the universe.
    pub(crate) fn explain(&self, size_decimals: impl Fn(&str) -> Option<u32>) -> Result<TakeoverPool, String> {
        let [record] = self.records.as_slice() else {
            return Err(format!("{} takeover records in the block", self.records.len()));
        };
        if record.notional_wei <= 0 {
            return Err(format!("liquidatedNtlPos {} wei", record.notional_wei));
        }
        if self.fills.len() != record.positions.len() {
            return Err(format!("{} backstop fills for {} listed positions", self.fills.len(), record.positions.len()));
        }
        let mut notional_micro = 0_i128;
        for (coin, szi) in &record.positions {
            let mut of_coin = self.fills.iter().filter(|fill| fill.coin == *coin);
            let (Some(fill), None) = (of_coin.next(), of_coin.next()) else {
                return Err(format!("{coin}: not one backstop fill"));
            };
            notional_micro += taken_position(record, fill, szi, &size_decimals).map_err(|err| format!("{coin}: {err}"))?;
        }
        if notional_micro * i128::from(WEI_PER_MICRO) != i128::from(record.notional_wei) {
            return Err(format!("notional at mark {notional_micro} micro USDC, liquidatedNtlPos {} wei", record.notional_wei));
        }
        Ok(record.pool)
    }
}

/// What a backstop fill (not ADL) is to the account it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Backstop {
    /// The liquidated side, with its block's evidence.
    Liquidated(Arc<TakeoverEvidence>),
    /// The liquidator's side.
    Liquidator,
    /// A fill not checked against its block, or whose liquidated account does not read (the text says which).
    Unchecked(String),
}

impl Backstop {
    /// The pool the takeover empties, or why the cash replay cannot read this fill.
    pub(crate) fn takeover(&self, size_decimals: impl Fn(&str) -> Option<u32>) -> Result<TakeoverPool, String> {
        match self {
            Self::Liquidated(evidence) => evidence.explain(size_decimals),
            Self::Liquidator => Err("liquidator".to_string()),
            Self::Unchecked(why) => Err(why.clone()),
        }
    }
}

/// A block's takeover evidence, by liquidated account.
#[derive(Debug, Default)]
pub(crate) struct Takeovers(FxHashMap<Address, Arc<TakeoverEvidence>>);

impl Takeovers {
    pub(crate) fn of<'a>(events: impl Iterator<Item = &'a MiscEvent>, fills: impl Iterator<Item = &'a NodeDataFill>) -> Self {
        let mut evidence: FxHashMap<Address, TakeoverEvidence> = FxHashMap::default();
        for event in events {
            if let MiscEvent::Ledger { users, delta: LedgerDelta::Liquidation(record) } = event {
                for user in users {
                    evidence.entry(*user).or_default().records.push(record.clone());
                }
            }
        }
        for NodeDataFill(user, fill) in fills {
            if let (Some(Ok(liquidated)), Some(liquidation)) = (liquidated_user(fill), &fill.liquidation)
                && liquidated == *user
            {
                evidence.entry(*user).or_default().fills.push(TakenFill {
                    coin: fill.coin.clone(),
                    side: fill.side,
                    px: fill.px.clone(),
                    sz: fill.sz.clone(),
                    start_position: fill.start_position.clone(),
                    mark_px: liquidation.mark_px.clone(),
                });
            }
        }
        Self(evidence.into_iter().map(|(user, evidence)| (user, Arc::new(evidence))).collect())
    }

    /// `None` for a fill other than a backstop one (ADL included).
    pub(crate) fn backstop(&self, user: Address, fill: &Fill) -> Option<Backstop> {
        Some(match liquidated_user(fill)? {
            Ok(liquidated) if liquidated == user => self.0.get(&user).map_or_else(
                || Backstop::Unchecked("not in the block's fills".to_string()),
                |evidence| Backstop::Liquidated(Arc::clone(evidence)),
            ),
            Ok(_) => Backstop::Liquidator,
            Err(why) => Backstop::Unchecked(why),
        })
    }
}

/// A backstop fill not checked against its block, whichever side; `None` for any other fill.
pub(crate) fn unchecked(fill: &Fill) -> Option<Backstop> {
    liquidated_user(fill).map(|_| Backstop::Unchecked("not checked against its block".to_string()))
}

/// The liquidated account of a backstop fill (not ADL); `None` for any other fill.
fn liquidated_user(fill: &Fill) -> Option<Result<Address, String>> {
    let liquidation = fill.liquidation.as_ref().filter(|liquidation| liquidation.method == BACKSTOP_METHOD && fill.dir != ADL_DIR)?;
    Some(liquidation.liquidated_user.parse().map_err(|err| format!("liquidatedUser {:?}: {err}", liquidation.liquidated_user)))
}

/// Checks that `fill` takes the whole of `szi` at the rule's price; returns its notional at mark, micro USDC (a perp's
/// size and price decimals add to six).
fn taken_position(record: &TakeoverRecord, fill: &TakenFill, szi: &str, size_decimals: &impl Fn(&str) -> Option<u32>) -> Result<i128, String> {
    let decimals = size_decimals(&fill.coin).ok_or("outside the anchor's universe")?;
    let price_decimals = MAX_PERP_DECIMALS - decimals;
    let size = parse_size_raw(szi, decimals)?;
    let closing_side = if size > 0 { Side::Ask } else { Side::Bid };
    if size == 0 || parse_size_raw(&fill.start_position, decimals)? != size || parse_size_raw(&fill.sz, decimals)? != size.abs()
        || fill.side != closing_side
    {
        return Err(format!("the fill ({:?} {} from {}) does not take the whole {szi}", fill.side, fill.sz, fill.start_position));
    }
    let mark = i128::from(parse_size_raw(&fill.mark_px, price_decimals)?);
    let px = i128::from(parse_size_raw(&fill.px, price_decimals)?);
    // mark × (N − sign × V) / N in price units, either rounding
    let notional = i128::from(record.notional_wei);
    let signed_value = if size > 0 { i128::from(record.value_wei) } else { -i128::from(record.value_wei) };
    let scaled = mark * (notional - signed_value);
    let floor = scaled.div_euclid(notional);
    let ceil = floor + i128::from(scaled.rem_euclid(notional) != 0);
    if px != floor && px != ceil {
        return Err(format!("px {} off the takeover price, mark {} discounted by {} / {} wei", fill.px, fill.mark_px, record.value_wei, record.notional_wei));
    }
    Ok(i128::from(size.abs()) * mark)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::types::node_data::Batch;

    const LIQUIDATOR: &str = "0x4000000000000000000000000000000000000001";

    fn goldens() -> Vec<Value> {
        let golden: Value = serde_json::from_str(include_str!("backstop_goldens.json")).unwrap();
        golden["cases"].as_array().unwrap().clone()
    }

    fn decimals(coin: &str) -> Option<u32> {
        match coin {
            "xyz:AMZN" => Some(3),
            "xyz:INTC" => Some(2),
            _ => None,
        }
    }

    fn misc(case: &Value) -> Vec<MiscEvent> {
        serde_json::from_value::<Batch<MiscEvent>>(case["misc"].clone()).unwrap().events()
    }

    fn fills(case: &Value) -> Vec<NodeDataFill> {
        case["fills"].as_array().unwrap().iter()
            .flat_map(|line| serde_json::from_value::<Batch<NodeDataFill>>(line.clone()).unwrap().events())
            .collect()
    }

    fn takeovers(events: &[MiscEvent], fills: &[NodeDataFill]) -> Takeovers {
        Takeovers::of(events.iter(), fills.iter())
    }

    /// Each liquidated fill's reading against the fixed universe.
    fn readings(events: &[MiscEvent], fills: &[NodeDataFill]) -> Vec<Result<TakeoverPool, String>> {
        let takeovers = takeovers(events, fills);
        liquidated(fills).iter().map(|NodeDataFill(user, fill)| takeovers.backstop(*user, fill).unwrap().takeover(decimals)).collect()
    }

    fn liquidated(fills: &[NodeDataFill]) -> Vec<&NodeDataFill> {
        fills.iter().filter(|NodeDataFill(user, _)| *user != LIQUIDATOR.parse::<Address>().unwrap()).collect()
    }

    #[test]
    fn every_golden_takeover_is_explained_by_the_rule() {
        let cases = goldens();
        assert_eq!(cases.len(), 4);
        for case in &cases {
            let (events, fills) = (misc(case), fills(case));
            let pool = if case["case"].as_str().unwrap().starts_with("cross") { TakeoverPool::Cross } else { TakeoverPool::Isolated };
            let takeovers = takeovers(&events, &fills);
            for NodeDataFill(user, fill) in &fills {
                let backstop = takeovers.backstop(*user, fill).unwrap();
                if *user == LIQUIDATOR.parse::<Address>().unwrap() {
                    assert_eq!(backstop, Backstop::Liquidator, "{}", case["case"]);
                } else {
                    assert_eq!(backstop.takeover(decimals), Ok(pool), "{}", case["case"]);
                }
            }
        }
    }

    #[test]
    fn the_record_parses_as_the_wire_writes_it() {
        let events = misc(&goldens()[2]);
        let [MiscEvent::Ledger { users, delta: LedgerDelta::Liquidation(record) }] = events.as_slice() else { panic!("{events:?}") };
        assert_eq!(users, &["0x937613997fb4119e563baee0f2122b753bb2633f".parse::<Address>().unwrap()]);
        assert_eq!(record, &TakeoverRecord {
            pool: TakeoverPool::Cross,
            value_wei: 4_890_198_400,
            notional_wei: 194_565_688_000,
            positions: vec![("xyz:INTC".to_string(), "12.28".to_string()), ("xyz:AMZN".to_string(), "3.649".to_string())],
        });
    }

    /// The other rounding is accepted; a price a unit further, another size, a missing or doubled record, a lost
    /// position fill, a notional that does not add up or a coin outside the universe is not.
    #[test]
    fn a_takeover_the_record_does_not_explain_is_unexplained() {
        let cross = &goldens()[2];
        let (events, fills) = (misc(cross), fills(cross));
        let unexplained = |events: &[MiscEvent], fills: &[NodeDataFill]| -> Vec<String> {
            readings(events, fills).into_iter().map(Result::unwrap_err).collect()
        };
        let edit = |fills: &[NodeDataFill], coin: &str, change: &dyn Fn(&mut Fill)| -> Vec<NodeDataFill> {
            fills.iter().cloned().map(|NodeDataFill(user, mut fill)| {
                if fill.coin == coin && user != LIQUIDATOR.parse::<Address>().unwrap() { change(&mut fill); }
                NodeDataFill(user, fill)
            }).collect()
        };
        // the rule's INTC price is 86.88200..., rounded down to 86.882; up is the other rounding
        let up = edit(&fills, "xyz:INTC", &|fill| fill.px = "86.8821".to_string());
        assert!(readings(&events, &up).iter().all(Result::is_ok));
        let further = edit(&fills, "xyz:INTC", &|fill| fill.px = "86.8822".to_string());
        assert!(unexplained(&events, &further).iter().all(|why| why.contains("xyz:INTC: px 86.8822 off the takeover price")));
        let smaller = edit(&fills, "xyz:AMZN", &|fill| fill.sz = "3.648".to_string());
        assert!(unexplained(&events, &smaller)[0].contains("does not take the whole 3.649"));
        assert_eq!(unexplained(&[], &fills), vec!["0 takeover records in the block"; 2]);
        let doubled = [events.clone(), events.clone()].concat();
        assert_eq!(unexplained(&doubled, &fills), vec!["2 takeover records in the block"; 2]);
        let lost: Vec<NodeDataFill> = fills.iter().filter(|NodeDataFill(_, fill)| fill.coin != "xyz:AMZN").cloned().collect();
        assert_eq!(unexplained(&events, &lost), vec!["1 backstop fills for 2 listed positions"]);
        let mut off_notional = events.clone();
        let MiscEvent::Ledger { delta: LedgerDelta::Liquidation(record), .. } = &mut off_notional[0] else { unreachable!() };
        record.notional_wei += WEI_PER_MICRO;
        assert!(unexplained(&off_notional, &fills).iter().all(|why| why.starts_with("notional at mark 1945656880 micro USDC")));
        let takeovers = takeovers(&events, &fills);
        let NodeDataFill(user, fill) = liquidated(&fills)[0];
        let outside = takeovers.backstop(*user, fill).unwrap().takeover(|_| None);
        assert!(matches!(outside, Err(why) if why.contains("outside the anchor's universe")));
    }

    #[test]
    fn adl_and_ordinary_fills_are_not_backstop_fills() {
        let fills = fills(&goldens()[0]);
        let takeovers = takeovers(&[], &fills);
        let NodeDataFill(user, fill) = liquidated(&fills)[0];
        let mut adl = fill.clone();
        adl.dir = ADL_DIR.to_string();
        assert_eq!(takeovers.backstop(*user, &adl), None);
        let mut ordinary = fill.clone();
        ordinary.liquidation = None;
        assert_eq!(takeovers.backstop(*user, &ordinary), None);
        let mut market = fill.clone();
        market.liquidation = Some(serde_json::from_value(json!({"liquidatedUser": user.to_string(), "markPx": "1", "method": "market"})).unwrap());
        assert_eq!(takeovers.backstop(*user, &market), None);
    }
}
