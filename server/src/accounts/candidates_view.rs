//! The served `extLiquidationCandidates` feed (design 000443 r1 section 5, r2 revision 5): each subscription projects the
//! server's candidate set (`candidates::MAX_CANDIDATE_DISTANCE_BPS`) onto its own coin and distance, with its own
//! version. A candidate enters or exits a subscription as its published distance crosses the subscription's threshold.

use alloy::primitives::Address;
use rustc_hash::FxHashSet;
use serde::Serialize;

use super::{
    candidates::{BookWalk, Candidate, CandidateChanges, CandidatesSnapshot, walk_as_of},
    checkpoint::MAX_PERP_DECIMALS,
    cooldown::Cooldown,
    size::format_size_raw,
    view::user_text,
    walk::{WalkEnd, WalkPart},
};
use crate::{order_book::Sz, types::subscription::EXT_LIQUIDATION_CANDIDATES};

/// Micro-USD notionals are printed with six decimals.
const MICRO_DECIMALS: u32 = 6;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CandidateDto {
    user: String,
    coin: String,
    szi: String,
    mark_px: String,
    liquidation_px: String,
    liquidation_distance_bps: f64,
    /// The block whose state the row is of; a candidate not updated since keeps the height it was sent with.
    liquidation_px_height: u64,
    margin_mode: &'static str,
    pool_positions: usize,
    cross_pool_coins: Vec<String>,
    expected_order: ExpectedOrderDto,
    book_walk: BookWalkDto,
}

/// The static walk of the public book (design 000443 r2 revisions 3–4, r3 revisions 1–2, 4).
#[expect(clippy::large_enum_variant, reason = "a short-lived DTO, built per row as it is serialized")]
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum BookWalkDto {
    /// This process keeps no order book.
    NoBook,
    /// The book is not trusted: no estimate.
    Untrusted,
    /// Not walked yet.
    Pending,
    Walked {
        /// The book's mid at the copy, USD; `null` with an empty side.
        mid_px: Option<String>,
        /// Its share of the cumulative walk, after the candidates before it (`aheadSize`).
        together: WalkPartDto,
        /// Its size walked alone from the best level.
        alone: WalkPartDto,
        ahead_size: String,
        /// `unknownCooldownAsFull` when this candidate or one before it walks a whole position for an unknown cooldown.
        assumption: &'static str,
        book_height: u64,
        book_time: u64,
    },
}

/// One walk: sizes in the coin's units, prices in USD; `null` prices when nothing filled.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WalkPartDto {
    vwap_px: Option<String>,
    deepest_px: Option<String>,
    /// How far the deepest level is from the mid, bp; `null` without a mid or a fill.
    deepest_bps_from_mid: Option<f64>,
    filled_size: String,
    levels: usize,
    /// `filled`, `bookExhausted`, or `truncatedAtLevels` (the copy stopped at 2,000 levels, the book may hold more).
    end: &'static str,
}

impl WalkPartDto {
    fn new(part: &WalkPart, mid_px: Option<f64>) -> Self {
        let deepest = part.deepest_px.map(|px| px.value() as f64 / 1e8);
        Self {
            vwap_px: part.vwap_px.map(|px| px.to_string()),
            deepest_px: part.deepest_px.map(crate::order_book::Px::to_str),
            deepest_bps_from_mid: deepest.zip(mid_px).map(|(deepest, mid)| (deepest - mid).abs() / mid * 10_000.0),
            filled_size: Sz::new(part.filled).to_str(),
            levels: part.levels,
            end: match part.end {
                WalkEnd::Filled => "filled",
                WalkEnd::BookExhausted => "bookExhausted",
                WalkEnd::TruncatedAtLevels => "truncatedAtLevels",
            },
        }
    }
}

impl BookWalkDto {
    fn new(walk: &BookWalk) -> Self {
        let BookWalk::Walked { share, unknown_cooldown_as_full, book_height, book_time, mid_px, .. } = walk else {
            return match walk {
                BookWalk::NoBook => Self::NoBook,
                BookWalk::Untrusted => Self::Untrusted,
                BookWalk::Pending | BookWalk::Walked { .. } => Self::Pending,
            };
        };
        Self::Walked {
            mid_px: mid_px.map(|px| px.to_string()),
            together: WalkPartDto::new(&share.together, *mid_px),
            alone: WalkPartDto::new(&share.alone, *mid_px),
            ahead_size: Sz::new(share.ahead).to_str(),
            assumption: if *unknown_cooldown_as_full { "unknownCooldownAsFull" } else { "known" },
            book_height: *book_height,
            book_time: *book_time,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedOrderDto {
    /// `A` sells a long, `B` buys back a short.
    side: &'static str,
    cooldown: &'static str,
    first_order_size: String,
    cooldown_order_size: String,
    walk_size: String,
    notional: String,
}

impl CandidateDto {
    /// `walk` is the candidate's walk as it may be published now (`walk_as_of`).
    fn new(coin: &str, user: Address, candidate: &Candidate, walk: &BookWalk) -> Self {
        let size = |raw: u64| format_size_raw(raw.cast_signed(), candidate.size_decimals);
        let order = &candidate.order;
        #[expect(clippy::expect_used, reason = "a notional above 9.2e12 USD is not a position")]
        let notional_micro = i64::try_from(order.notional_micro).expect("a notional in i64 micro-USD");
        Self {
            user: user_text(user),
            coin: coin.to_string(),
            szi: format_size_raw(candidate.size_raw, candidate.size_decimals),
            mark_px: format_size_raw(candidate.mark_raw.cast_signed(), MAX_PERP_DECIMALS - candidate.size_decimals),
            liquidation_px: candidate.liquidation_px.to_string(),
            liquidation_distance_bps: candidate.distance_bps,
            liquidation_px_height: candidate.height,
            margin_mode: if candidate.isolated { "isolated" } else { "cross" },
            pool_positions: candidate.pool_positions,
            cross_pool_coins: candidate.cross_pool_coins.clone(),
            expected_order: ExpectedOrderDto {
                side: if candidate.size_raw > 0 { "A" } else { "B" },
                cooldown: match order.cooldown {
                    Cooldown::Active => "inCooldown",
                    Cooldown::None => "none",
                    Cooldown::Unknown => "unknown",
                },
                first_order_size: size(order.first_order_raw),
                cooldown_order_size: size(order.cooldown_order_raw),
                walk_size: size(order.walk_raw),
                notional: format_size_raw(notional_micro, MICRO_DECIMALS),
            },
            book_walk: BookWalkDto::new(walk),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct ExitDto {
    user: String,
    coin: String,
}

/// One frame of a subscription, named by its parameters.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CandidatesFrame {
    coin: Option<String>,
    max_distance_bps: u32,
    #[serde(flatten)]
    body: CandidatesBody,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum CandidatesBody {
    Snapshot { version: u64, height: u64, time: u64, candidates: Vec<CandidateDto> },
    Changes { version: u64, height: u64, time: u64, enter: Vec<CandidateDto>, update: Vec<CandidateDto>, exit: Vec<ExitDto> },
    /// The subscription ended; subscribe again for a new snapshot once the feed is available.
    Withdrawn { reason: String },
}

impl CandidatesFrame {
    /// The wire frame `{"channel":"extLiquidationCandidates","data":<frame>}`.
    #[expect(clippy::expect_used, reason = "these DTOs have string keys and finite numbers only; serializing cannot fail")]
    pub(crate) fn to_wire(&self) -> bytes::Bytes {
        #[derive(Serialize)]
        struct Envelope<'a> {
            channel: &'static str,
            data: &'a CandidatesFrame,
        }
        bytes::Bytes::from(serde_json::to_vec(&Envelope { channel: EXT_LIQUIDATION_CANDIDATES, data: self }).expect("serializable DTOs"))
    }
}

/// One owner refresh in full (every candidate within the server's distance), for the follow probe's record.
pub(crate) fn changes_json(changes: &CandidateChanges) -> serde_json::Value {
    let rows = |rows: &[(String, Address, Candidate)]| -> Vec<CandidateDto> {
        // the follow probe keeps no book: its walks are published as they are
        rows.iter().map(|(coin, user, candidate)| CandidateDto::new(coin, *user, candidate, &candidate.walk)).collect()
    };
    let exit: Vec<ExitDto> = changes.exit.iter().map(|(coin, user)| ExitDto { user: user_text(*user), coin: coin.clone() }).collect();
    serde_json::json!({
        "version": changes.version, "height": changes.height, "time": changes.time_ms,
        "enter": rows(&changes.enter), "update": rows(&changes.update), "exit": exit,
    })
}

/// A subscription's parameters: one coin or all, and a distance up to `MAX_CANDIDATE_DISTANCE_BPS` (checked when the
/// subscription is validated).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateFilter {
    pub(crate) coin: Option<String>,
    pub(crate) max_distance_bps: u32,
}

impl CandidateFilter {
    fn holds(&self, coin: &str, candidate: &Candidate) -> bool {
        self.coin.as_deref().is_none_or(|wanted| wanted == coin) && candidate.distance_bps <= f64::from(self.max_distance_bps)
    }

    fn frame(&self, body: CandidatesBody) -> CandidatesFrame {
        CandidatesFrame { coin: self.coin.clone(), max_distance_bps: self.max_distance_bps, body }
    }

    /// The frame ending this subscription.
    pub(crate) fn withdrawn(&self, reason: &str) -> CandidatesFrame {
        self.frame(CandidatesBody::Withdrawn { reason: reason.to_string() })
    }
}

/// One subscription's view of the server's candidates.
#[derive(Debug)]
pub(crate) struct CandidateProjection {
    filter: CandidateFilter,
    /// The server version it stands at.
    server_version: u64,
    /// Its own frames' version.
    version: u64,
    members: FxHashSet<(String, Address)>,
}

impl CandidateProjection {
    /// The subscription at a snapshot, and its first frame; `book` is the order book's (trusted, withheld periods) now.
    pub(crate) fn subscribe(filter: CandidateFilter, snapshot: &CandidatesSnapshot, book: Option<(bool, u64)>) -> (Self, CandidatesFrame) {
        let held: Vec<&(String, Address, Candidate)> =
            snapshot.candidates.iter().filter(|(coin, _, candidate)| filter.holds(coin, candidate)).collect();
        let members = held.iter().map(|(coin, user, _)| (coin.clone(), *user)).collect();
        let candidates = held.iter().map(|(coin, user, candidate)| CandidateDto::new(coin, *user, candidate, &walk_as_of(&candidate.walk, book))).collect();
        let frame = filter.frame(CandidatesBody::Snapshot { version: 0, height: snapshot.height, time: snapshot.time_ms, candidates });
        (Self { filter, server_version: snapshot.version, version: 0, members }, frame)
    }

    /// The frame ending this subscription.
    pub(crate) fn withdrawn(&self, reason: &str) -> CandidatesFrame {
        self.filter.withdrawn(reason)
    }

    /// The subscription's frame for a server changes frame: `Ok(None)` when it changes nothing here (or is older than
    /// the snapshot), `Err` with the reason on a gap.
    /// `book` as for `subscribe`, read when the frame goes out: a walk estimated before the book was withheld does not
    /// go out as one (r3 revision 2).
    pub(crate) fn apply(&mut self, changes: &CandidateChanges, book: Option<(bool, u64)>) -> Result<Option<CandidatesFrame>, String> {
        if changes.version <= self.server_version {
            return Ok(None);
        }
        if changes.version != self.server_version + 1 {
            return Err(format!("candidate frames {} to {} missed", self.server_version + 1, changes.version - 1));
        }
        self.server_version = changes.version;
        let (mut enter, mut update, mut exit) = (Vec::new(), Vec::new(), Vec::new());
        for (coin, user, candidate) in changes.enter.iter().chain(&changes.update) {
            let key = (coin.clone(), *user);
            match (self.filter.holds(coin, candidate), self.members.contains(&key)) {
                (true, false) => {
                    self.members.insert(key);
                    enter.push(CandidateDto::new(coin, *user, candidate, &walk_as_of(&candidate.walk, book)));
                }
                (true, true) => update.push(CandidateDto::new(coin, *user, candidate, &walk_as_of(&candidate.walk, book))),
                (false, true) => {
                    self.members.remove(&key);
                    exit.push(ExitDto { user: user_text(*user), coin: coin.clone() });
                }
                (false, false) => {}
            }
        }
        for (coin, user) in &changes.exit {
            if self.members.remove(&(coin.clone(), *user)) {
                exit.push(ExitDto { user: user_text(*user), coin: coin.clone() });
            }
        }
        if enter.is_empty() && update.is_empty() && exit.is_empty() {
            return Ok(None);
        }
        self.version += 1;
        let body = CandidatesBody::Changes { version: self.version, height: changes.height, time: changes.time_ms, enter, update, exit };
        Ok(Some(self.filter.frame(body)))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::accounts::candidates::ExpectedOrder;

    const A: Address = Address::repeat_byte(0xa1);
    const B: Address = Address::repeat_byte(0xb2);

    fn candidate(distance_bps: f64) -> Candidate {
        Candidate {
            size_raw: -2_000_000,
            size_decimals: 1,
            height: 7,
            mark_raw: 400,
            liquidation_px: 0.004_1,
            distance_bps,
            isolated: false,
            pool_positions: 2,
            cross_pool_coins: vec!["BTC".to_string()],
            order: ExpectedOrder {
                cooldown: Cooldown::Unknown,
                first_order_raw: 2_000_000,
                cooldown_order_raw: 2_000_000,
                walk_raw: 2_000_000,
                notional_micro: 800_000_000,
            },
            walk: BookWalk::NoBook,
        }
    }

    fn changes(version: u64, enter: Vec<(&str, Address, f64)>, update: Vec<(&str, Address, f64)>, exit: Vec<(&str, Address)>) -> CandidateChanges {
        let rows = |rows: Vec<(&str, Address, f64)>| {
            rows.into_iter().map(|(coin, user, distance)| (coin.to_string(), user, candidate(distance))).collect()
        };
        CandidateChanges {
            version,
            height: 8,
            time_ms: 8_000,
            enter: rows(enter),
            update: rows(update),
            exit: exit.into_iter().map(|(coin, user)| (coin.to_string(), user)).collect(),
        }
    }

    fn json(frame: &CandidatesFrame) -> serde_json::Value {
        serde_json::from_slice(&frame.to_wire()).unwrap()
    }

    fn users(frame: &serde_json::Value, list: &str) -> Vec<String> {
        frame["data"][list].as_array().unwrap().iter().map(|row| row["user"].as_str().unwrap().to_string()).collect()
    }

    /// r2 revision 5: the server set (1,000 bp) projects onto the subscription's coin and threshold; a distance crossing
    /// the threshold enters or exits it; versions run on per subscription; a gap is an error, an older frame nothing.
    #[test]
    fn a_subscription_projects_the_server_candidates() {
        let snapshot = CandidatesSnapshot {
            version: 3,
            height: 7,
            time_ms: 7_000,
            candidates: vec![("PUMP".to_string(), A, candidate(150.0)), ("PUMP".to_string(), B, candidate(400.0)), ("BTC".to_string(), A, candidate(50.0))],
        };
        let filter = CandidateFilter { coin: Some("PUMP".to_string()), max_distance_bps: 200 };
        let (mut projection, first) = CandidateProjection::subscribe(filter, &snapshot, None);
        let first = json(&first);
        assert_eq!((first["channel"].as_str(), first["data"]["kind"].as_str(), first["data"]["version"].as_u64()), (Some("extLiquidationCandidates"), Some("snapshot"), Some(0)));
        assert_eq!((first["data"]["coin"].as_str(), first["data"]["maxDistanceBps"].as_u64()), (Some("PUMP"), Some(200)));
        assert_eq!(users(&first, "candidates"), [user_text(A)]);
        assert!(matches!(projection.apply(&changes(3, vec![], vec![("PUMP", A, 300.0)], vec![]), None), Ok(None)), "in the snapshot already");
        // A moves out of 200 bp, B moves in, BTC is another coin
        let frame = projection.apply(&changes(4, vec![("BTC", B, 10.0)], vec![("PUMP", A, 250.0), ("PUMP", B, 180.0)], vec![]), None).unwrap().unwrap();
        let frame = json(&frame);
        assert_eq!((frame["data"]["kind"].as_str(), frame["data"]["version"].as_u64()), (Some("changes"), Some(1)));
        assert_eq!((users(&frame, "enter"), users(&frame, "update"), users(&frame, "exit")), (vec![user_text(B)], vec![], vec![user_text(A)]));
        let frame = json(&projection.apply(&changes(5, vec![], vec![("PUMP", B, 170.0)], vec![("PUMP", A)]), None).unwrap().unwrap());
        assert_eq!((users(&frame, "update"), users(&frame, "exit"), frame["data"]["version"].as_u64()), (vec![user_text(B)], vec![], Some(2)), "A had left");
        assert!(matches!(projection.apply(&changes(7, vec![], vec![], vec![]), None), Err(reason) if reason == "candidate frames 6 to 6 missed"));
    }

    /// r1 section 5 / r2 revision 2: a candidate's served fields.
    #[test]
    fn serializes_a_walk_together_and_alone_with_the_distance_from_the_mid() {
        let unit = 100_000_000;
        let part = |vwap_px: f64, deepest: u64| WalkPart {
            filled: 2 * unit,
            vwap_px: Some(vwap_px),
            deepest_px: Some(crate::order_book::Px::new(deepest)),
            levels: 2,
            end: WalkEnd::Filled,
        };
        let walk = BookWalk::Walked {
            share: crate::accounts::walk::WalkShare { ahead: unit, together: part(100.75, 101 * unit), alone: part(100.25, 1005 * unit / 10) },
            unknown_cooldown_as_full: false,
            book_height: 9,
            book_time: 10,
            withheld_periods: 0,
            mid_px: Some(100.0),
        };
        let walked = serde_json::to_value(BookWalkDto::new(&walk)).unwrap();
        assert_eq!((walked["status"].as_str(), walked["midPx"].as_str(), walked["aheadSize"].as_str()), (Some("walked"), Some("100"), Some("1")));
        let parts = (&walked["together"], &walked["alone"]);
        assert_eq!((parts.0["deepestBpsFromMid"].as_f64(), parts.1["deepestBpsFromMid"].as_f64()), (Some(100.0), Some(50.0)));
        assert_eq!((parts.0["vwapPx"].as_str(), parts.1["deepestPx"].as_str(), parts.1["end"].as_str()), (Some("100.75"), Some("100.5"), Some("filled")));
        let BookWalk::Walked { share, .. } = walk else { unreachable!() };
        let no_mid = BookWalk::Walked { share, unknown_cooldown_as_full: false, book_height: 9, book_time: 10, withheld_periods: 0, mid_px: None };
        assert_eq!(serde_json::to_value(BookWalkDto::new(&no_mid)).unwrap()["alone"]["deepestBpsFromMid"], serde_json::Value::Null);
    }

    #[test]
    fn serializes_a_candidate_with_its_expected_order() {
        let snapshot = CandidatesSnapshot { version: 1, height: 7, time_ms: 7_000, candidates: vec![("PUMP".to_string(), A, candidate(150.25))] };
        let (_, frame) = CandidateProjection::subscribe(CandidateFilter { coin: None, max_distance_bps: 1_000 }, &snapshot, None);
        assert_eq!(
            json(&frame)["data"]["candidates"][0],
            json!({"user": user_text(A), "coin": "PUMP", "szi": "-200000.0", "markPx": "0.004", "liquidationPx": "0.0041",
                   "liquidationDistanceBps": 150.25, "liquidationPxHeight": 7, "marginMode": "cross", "poolPositions": 2,
                   "crossPoolCoins": ["BTC"],
                   "expectedOrder": {"side": "B", "cooldown": "unknown", "firstOrderSize": "200000.0",
                                     "cooldownOrderSize": "200000.0", "walkSize": "200000.0", "notional": "800.0"},
                   "bookWalk": {"status": "noBook"}})
        );
        assert_eq!(json(&CandidateFilter { coin: None, max_distance_bps: 50 }.withdrawn("gone"))["data"], json!({"coin": null, "maxDistanceBps": 50, "kind": "withdrawn", "reason": "gone"}));
    }
}
