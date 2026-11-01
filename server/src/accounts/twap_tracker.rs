//! The checkpoint's running TWAPs (hyperliquid-trade design 000624 r4 section 1; batch 2h): each perp dex's
//! `twap_tracker` and `exchange.spot_twap_tracker`, of each its `user_to_id_to_state` - `running_heap` and the TWAPs
//! waiting for their trigger (`user_to_id_to_waiting`) are not read. The pending book seeds its TWAPs from them.
//!
//! A state is `{"u", "s": {"a", "s", "z", "d", "sp", "R"?, "r"?}, "t", "n", "e", "E", "S"}` (bm checkpoint 1204910000:
//! 322 perp and 142 spot TWAPs): the asset in the tracker's own numbering - a perp dex `d`'s `10_000 * d + index`, the
//! spot tracker's the pair index -, the side `B` / `A`, the total and executed sizes in `szDecimals` units, the duration
//! in seconds, the parent's max buy / min sell price in integer price units (`sp`, `null` without one), randomize and
//! reduce-only (written only when true), the start (unix ms) and the next suborder's slot `S` (research
//! `mempool_pending_book_20261031` "TWAP 子单的时刻与数量": 33 of 33 against the node's TWAP statuses).

use alloy::primitives::Address;
use serde::Deserialize;

/// A perp dex `d >= 1`'s action asset ids start here (`100_000 + 10_000 * d + index`, HL docs "Asset IDs"); its
/// tracker writes `10_000 * d + index`.
const HIP3_ASSET_BASE: u32 = 100_000;
/// A spot pair's action asset id is `10_000 + index`; the spot tracker writes the index.
const SPOT_ASSET_BASE: u32 = 10_000;

/// A running TWAP as of the checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointTwap {
    pub(crate) id: u64,
    pub(crate) user: Address,
    /// The action asset id.
    pub(crate) asset: u32,
    pub(crate) is_buy: bool,
    /// In `szDecimals` units.
    pub(crate) total: u64,
    pub(crate) executed: u64,
    pub(crate) duration_s: u64,
    pub(crate) start_ms: u64,
    /// When its next suborder is due, unix ms (`n`): on its own grid of 30 s - start + 1 s on bm's 401 of 422 TWAPs of
    /// 11-01 07:09, start + 0 s on 21 others (whose suborders ran before the first grid's points).
    pub(crate) next_due_ms: u64,
    /// `S`: the count its target takes at that slot - total x `S` / N (the slot itself on 56 of bm's 124 plain main-dex
    /// TWAPs of 11-01 08:33, one more on 68; the first suborder after the checkpoint as the node ran it on 35 against 29
    /// for slot + 1).
    pub(crate) slot_count: u64,
    pub(crate) randomize: bool,
    pub(crate) reduce_only: bool,
    /// Its max buy / min sell price in integer price units (`MAX_PERP_DECIMALS` or `MAX_SPOT_DECIMALS` minus
    /// `szDecimals` decimals).
    pub(crate) limit_px_raw: Option<u64>,
}

/// A `twap_tracker`, as read: its running TWAPs, and the ids of those waiting for their trigger - their suborder grid,
/// once activated, starts at the activation itself (research `twap_randomize_20261101`: 12 of 12 TWAPs seen go from
/// `waitingForTrigger` to `activated`, against start + 1 s for the 55 activated at once).
#[derive(Debug, Deserialize)]
pub(crate) struct WireTwapTracker {
    user_to_id_to_state: Vec<(Address, Vec<(u64, WireTwapState)>)>,
    user_to_id_to_waiting: Vec<(Address, Vec<(u64, serde::de::IgnoredAny)>)>,
}

#[derive(Debug, Deserialize)]
struct WireTwapState {
    u: Address,
    s: WireTwapSpec,
    t: u64,
    n: String,
    e: u64,
    #[serde(rename = "S")]
    slot_count: u64,
}

#[derive(Debug, Deserialize)]
struct WireTwapSpec {
    a: u32,
    s: String,
    z: u64,
    d: f64,
    sp: Option<u64>,
    #[serde(rename = "R", default)]
    randomize: bool,
    #[serde(rename = "r", default)]
    reduce_only: bool,
}

/// Which tracker a state comes from, for its asset's numbering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrackerOf {
    /// `perp_dexs[d]`.
    PerpDex(usize),
    Spot,
}

impl WireTwapTracker {
    /// The ids of the TWAPs waiting for their trigger.
    pub(crate) fn waiting_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.user_to_id_to_waiting.iter().flat_map(|(_, waiting)| waiting.iter().map(|(id, _)| *id))
    }

    /// Its running TWAPs, the assets as actions name them.
    pub(crate) fn twaps(self, of: TrackerOf) -> Result<Vec<CheckpointTwap>, String> {
        let mut twaps = Vec::new();
        for (user, states) in self.user_to_id_to_state {
            for (id, state) in states {
                twaps.push(state.twap(id, user, of).map_err(|err| format!("{of:?} twap {id}: {err}"))?);
            }
        }
        Ok(twaps)
    }
}

impl WireTwapState {
    fn twap(self, id: u64, user: Address, of: TrackerOf) -> Result<CheckpointTwap, String> {
        if self.u != user {
            return Err(format!("listed under {user}, its state's user {}", self.u));
        }
        let is_buy = match self.s.s.as_str() {
            "B" => true,
            "A" => false,
            other => return Err(format!("side {other:?}")),
        };
        // a duration of whole minutes (HL docs "TWAP details": 5 minutes to 7 days)
        let duration_s = self.s.d;
        if !(duration_s >= 60.0 && duration_s % 60.0 == 0.0) {
            return Err(format!("duration {duration_s} s, not whole minutes"));
        }
        let next_due_ms = super::checkpoint::node_time_ns(&self.n).map_err(|err| format!("n {:?}: {err}", self.n))? / 1_000_000;
        let asset = match of {
            TrackerOf::PerpDex(0) => self.s.a,
            TrackerOf::PerpDex(_) => HIP3_ASSET_BASE.checked_add(self.s.a).ok_or("asset past u32")?,
            TrackerOf::Spot => SPOT_ASSET_BASE.checked_add(self.s.a).ok_or("asset past u32")?,
        };
        Ok(CheckpointTwap {
            id,
            user,
            asset,
            is_buy,
            total: self.s.z,
            executed: self.e,
            #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "a whole positive number of seconds, checked above; past u64 saturates")]
            duration_s: duration_s as u64,
            start_ms: self.t,
            next_due_ms,
            slot_count: self.slot_count,
            randomize: self.s.randomize,
            reduce_only: self.s.reduce_only,
            limit_px_raw: self.s.sp,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use serde_json::json;

    use super::*;

    /// bm checkpoint 1204910000's TWAP 2470033 (HYPE, main dex), and one waiting for its trigger.
    pub(crate) fn tracker_json() -> serde_json::Value {
        json!({
            "running_heap": [{"next_slice_time": "2026-11-01T07:09:35.558176551", "user": "0x03ce7863a2b62f4e227fd98605b79beb32618c76", "twap_id": 2_470_033}],
            "user_to_id_to_state": [
                ["0x03ce7863a2b62f4e227fd98605b79beb32618c76", [[2_470_033, {"u": "0x03ce7863a2b62f4e227fd98605b79beb32618c76",
                    "s": {"a": 159, "s": "B", "z": 11_563, "d": 172_800.0, "sp": null}, "t": 1_793_456_104_558_u64,
                    "n": "2026-11-01T07:09:35.558176551", "e": 4074, "E": 3_578_660_410_u64, "S": 2030}]]]
            ],
            "user_to_id_to_waiting": [["0x0ef4e2bdf6c372c1e5998ab3aa9948d463b78c4a", [[2_092_847, {"u": "0x0ef4e2bdf6c372c1e5998ab3aa9948d463b78c4a",
                "t": {"p": 30_000}, "s": {"R": true, "a": 31, "s": "B", "z": 1_243_270, "d": 90_000.0, "sp": 33_000}, "T": 1_786_251_529_779_u64}]]]]
        })
    }

    /// A real checkpoint's anchor as the pending book takes it (`pending::twaps`' real-data replay).
    pub(crate) fn real_pending_anchor(path: &std::path::Path) -> crate::accounts::book::PendingAnchor {
        let checkpoint = crate::accounts::checkpoint::parse_checkpoint_file(path).unwrap();
        let anchor = crate::accounts::anchor::Anchor::from_checkpoint(&checkpoint).unwrap();
        let coins = crate::accounts::book::ActionCoins { coins: anchor.margins.action_coins() };
        crate::accounts::book::PendingAnchor { height: checkpoint.height, coins, twaps: checkpoint.twaps, waiting_twaps: checkpoint.waiting_twaps }
    }

    pub(crate) fn empty_tracker_json() -> serde_json::Value {
        json!({"running_heap": [], "user_to_id_to_state": [], "user_to_id_to_waiting": []})
    }

    fn parsed(value: serde_json::Value, of: TrackerOf) -> Result<Vec<CheckpointTwap>, String> {
        let tracker: WireTwapTracker = rmp_serde::from_slice(&rmp_serde::to_vec_named(&value).unwrap()).unwrap();
        tracker.twaps(of)
    }

    #[test]
    fn a_running_twap_reads_with_its_action_asset() {
        let twaps = parsed(tracker_json(), TrackerOf::PerpDex(0)).unwrap();
        let expected = CheckpointTwap {
            id: 2_470_033,
            user: "0x03ce7863a2b62f4e227fd98605b79beb32618c76".parse().unwrap(),
            asset: 159,
            is_buy: true,
            total: 11_563,
            executed: 4_074,
            duration_s: 172_800,
            start_ms: 1_793_456_104_558,
            next_due_ms: 1_793_516_975_558,
            slot_count: 2_030,
            randomize: false,
            reduce_only: false,
            limit_px_raw: None,
        };
        assert_eq!(twaps, [expected]);
        let tracker: WireTwapTracker = rmp_serde::from_slice(&rmp_serde::to_vec_named(&tracker_json()).unwrap()).unwrap();
        assert_eq!(tracker.waiting_ids().collect::<Vec<_>>(), [2_092_847]);
        assert_eq!(parsed(tracker_json(), TrackerOf::PerpDex(1)).unwrap()[0].asset, 100_159);
        assert_eq!(parsed(tracker_json(), TrackerOf::Spot).unwrap()[0].asset, 10_159);
        let mut flagged = tracker_json();
        flagged["user_to_id_to_state"][0][1][0][1]["s"] = json!({"R": true, "r": true, "a": 10_026, "s": "A", "z": 3199, "d": 4860.0, "sp": 498_000});
        let twap = parsed(flagged, TrackerOf::PerpDex(1)).unwrap().remove(0);
        assert_eq!((twap.asset, twap.is_buy, twap.randomize, twap.reduce_only, twap.limit_px_raw), (110_026, false, true, true, Some(498_000)));
    }

    #[test]
    fn a_state_not_as_written_does_not_read() {
        let with = |path: &[&str], value: serde_json::Value| {
            let mut tracker = tracker_json();
            let mut at = &mut tracker["user_to_id_to_state"][0][1][0][1];
            for key in &path[..path.len() - 1] {
                at = &mut at[*key];
            }
            at[path[path.len() - 1]] = value;
            tracker
        };
        assert!(parsed(with(&["u"], json!("0x0000000000000000000000000000000000000001")), TrackerOf::PerpDex(0)).is_err());
        assert!(parsed(with(&["s", "s"], json!("X")), TrackerOf::PerpDex(0)).is_err());
        assert!(parsed(with(&["s", "d"], json!(90.0)), TrackerOf::PerpDex(0)).is_err());
        let mut missing = tracker_json();
        missing.as_object_mut().unwrap().remove("user_to_id_to_state");
        assert!(rmp_serde::from_slice::<WireTwapTracker>(&rmp_serde::to_vec_named(&missing).unwrap()).is_err());
    }
}
