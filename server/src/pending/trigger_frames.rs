//! Batch 2l, second step (hyperliquid-trade design 000642 r1-r3): the trigger orders near each coin's mark, as the
//! forced flow's every-coin `NearWide` frames carry them - each one's effective size against its account's position -,
//! followed as a WS `extForcedFlow` subscriber follows them: a full query first and after a lag, then the broadcast's
//! frames, each coin's newest `seq` kept. A withdrawal of the forced flow, or its broadcast closing, ends them until
//! restart.

use std::{future::Future, pin::Pin, sync::Arc};

use log::error;
use rustc_hash::FxHashMap;
use tokio::sync::broadcast::{self, error::RecvError};

use crate::{
    accounts::{
        forced_flow::Tier,
        forced_flow_owner::{FlowContent, FlowDemandGuard, FlowFrame, ReadyFlow, TriggerStatus},
        publish::AccountsFrame,
        runtime::AccountsHandle,
    },
    metrics::PENDING_BOOK_TRIGGER_FRAMES_TOTAL,
};

/// The forced flow's tier the pending book follows: 50 bp of either mark (`Tier::NearWide`), every coin.
pub(crate) const TRIGGER_TIER: Tier = Tier::NearWide;

/// The pending thread's link to the forced flow: its demand of every coin's `TRIGGER_TIER` held, the account view's
/// broadcast (`None`: gone already) and the handle its full queries go through.
pub(crate) struct FlowFeed {
    _demand: FlowDemandGuard,
    pub(crate) frames: Option<broadcast::Receiver<Arc<AccountsFrame>>>,
    accounts: AccountsHandle,
}

impl FlowFeed {
    pub(crate) fn of(accounts: &AccountsHandle) -> Self {
        Self { _demand: accounts.acquire_flow((None, TRIGGER_TIER)), frames: accounts.frames(), accounts: accounts.clone() }
    }

    /// A full query of every coin's frame.
    pub(crate) fn query(&self) -> FlowQuery {
        let accounts = self.accounts.clone();
        Box::pin(async move { accounts.forced_flow(None, TRIGGER_TIER).await })
    }
}

/// A full query under way.
pub(crate) type FlowQuery = Pin<Box<dyn Future<Output = Result<Vec<Arc<FlowFrame>>, String>> + Send>>;

/// What a frame of the broadcast leaves to do.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Received {
    Taken,
    /// Frames were lost: a full query.
    QueryDue,
    /// The frames ended until restart: no more reading.
    Ended,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    /// The frames taken stand: each new one replaces its coin's.
    Following,
    /// A full query is under way (at the start, after a lag): every coin unknown until it answers.
    Querying,
    /// Withdrawn, closed or its query refused, until restart.
    Stopped(String),
}

/// Each coin's newest frame of `TRIGGER_TIER`, and whether they stand.
#[derive(Debug)]
pub(crate) struct TriggerFrames {
    state: State,
    frames: FxHashMap<String, Arc<FlowFrame>>,
}

impl Default for TriggerFrames {
    fn default() -> Self {
        Self { state: State::Querying, frames: FxHashMap::default() }
    }
}

impl TriggerFrames {
    /// The account view's broadcast's next: the forced flow's frames taken, its withdrawal or the broadcast's end ending
    /// them, a lag calling for a full query; any other frame passes.
    pub(crate) fn on_received(&mut self, received: Result<Arc<AccountsFrame>, RecvError>) -> Received {
        match received {
            Ok(frame) => match frame.as_ref() {
                AccountsFrame::ForcedFlow(flow) => {
                    self.on_frame(Arc::clone(flow));
                    Received::Taken
                }
                AccountsFrame::ForcedFlowWithdrawn { reason } => {
                    self.on_withdrawn(reason);
                    Received::Ended
                }
                _ => Received::Taken,
            },
            Err(RecvError::Lagged(_)) if self.on_lagged() => Received::QueryDue,
            Err(RecvError::Lagged(_)) => Received::Taken,
            Err(RecvError::Closed) => {
                self.on_closed();
                Received::Ended
            }
        }
    }

    /// A frame of the broadcast: taken when of the tier and newer than its coin's.
    pub(crate) fn on_frame(&mut self, frame: Arc<FlowFrame>) {
        if frame.tier != TRIGGER_TIER || matches!(self.state, State::Stopped(_)) {
            return;
        }
        PENDING_BOOK_TRIGGER_FRAMES_TOTAL.with_label_values(&["frame"]).inc();
        self.take(frame);
    }

    /// The broadcast lagged: frames lost, every coin unknown until a full query answers. Whether one is needed.
    pub(crate) fn on_lagged(&mut self) -> bool {
        if matches!(self.state, State::Stopped(_)) {
            return false;
        }
        PENDING_BOOK_TRIGGER_FRAMES_TOTAL.with_label_values(&["lagged"]).inc();
        self.state = State::Querying;
        true
    }

    /// A full query's answer: its frames taken - one of a `seq` already taken only leaves its coin's as it is - and the
    /// coins stand again (review 000642 r2: the answer ends the wait whatever its seqs); a refusal stops them.
    pub(crate) fn on_query(&mut self, answer: Result<Vec<Arc<FlowFrame>>, String>) {
        if matches!(self.state, State::Stopped(_)) {
            return;
        }
        match answer {
            Ok(frames) => {
                PENDING_BOOK_TRIGGER_FRAMES_TOTAL.with_label_values(&["query"]).inc();
                for frame in frames.into_iter().filter(|frame| frame.tier == TRIGGER_TIER) {
                    self.take(frame);
                }
                self.state = State::Following;
            }
            Err(reason) => self.stop("query_failed", format!("the forced flow's query failed: {reason}")),
        }
    }

    /// The forced flow ended until restart (`AccountsFrame::ForcedFlowWithdrawn`, review 000642 r1 P2-3).
    pub(crate) fn on_withdrawn(&mut self, reason: &str) {
        self.stop("withdrawn", format!("the forced flow was withdrawn: {reason}"));
    }

    /// The account view's broadcast closed.
    pub(crate) fn on_closed(&mut self) {
        self.stop("closed", "the account view's frames closed".to_string());
    }

    /// The coin's flow while its trigger orders are known whole, or why not.
    pub(crate) fn ready(&self, coin: &str) -> Result<&ReadyFlow, &'static str> {
        match self.state {
            State::Following => {}
            State::Querying => return Err("querying"),
            State::Stopped(_) => return Err("stopped"),
        }
        let frame = self.frames.get(coin).ok_or("no_frame")?;
        let FlowContent::Ready(ready) = &frame.content else { return Err("unavailable") };
        match ready.triggers {
            TriggerStatus::Complete => Ok(ready),
            TriggerStatus::Incomplete(_) => Err("incomplete"),
        }
    }

    fn take(&mut self, frame: Arc<FlowFrame>) {
        let newer = self.frames.get(&frame.coin).is_none_or(|taken| frame.seq > taken.seq);
        if newer {
            self.frames.insert(frame.coin.clone(), frame);
        }
    }

    fn stop(&mut self, event: &str, reason: String) {
        if matches!(self.state, State::Stopped(_)) {
            return;
        }
        error!("pending book: {reason}; no trigger order is executed until restart");
        PENDING_BOOK_TRIGGER_FRAMES_TOTAL.with_label_values(&[event]).inc();
        self.frames.clear();
        self.state = State::Stopped(reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        accounts::{forced_flow_owner::FlowHeights, publish::AccountsFrame},
        pending::triggers::tests::{ready, stop},
    };

    fn frame(coin: &str, tier: Tier, seq: u64, content: FlowContent) -> Arc<FlowFrame> {
        Arc::new(FlowFrame { coin: coin.to_string(), tier, seq, heights: FlowHeights::default(), content })
    }

    fn btc(seq: u64, trigger: &'static str) -> Arc<FlowFrame> {
        frame("BTC", TRIGGER_TIER, seq, FlowContent::Ready(Box::new(ready("100", vec![stop(seq, 1, trigger)]))))
    }

    fn broadcast(frame: Arc<FlowFrame>) -> Result<Arc<AccountsFrame>, RecvError> {
        Ok(Arc::new(AccountsFrame::ForcedFlow(frame)))
    }

    /// The coin's trigger orders, by the frame's first entry's oid (each fixture frame's `seq`).
    fn first_oid(frames: &TriggerFrames) -> Result<u64, &'static str> {
        frames.ready("BTC").map(|ready| match &ready.entries[0].entry.source {
            crate::accounts::forced_flow::Source::Trigger { order, .. } => order.oid,
            crate::accounts::forced_flow::Source::Liquidation { .. } => unreachable!(),
        })
    }

    /// Unknown until the first full query answers; then each newer frame of the tier replaces its coin's, an older one
    /// or one of another tier none.
    #[test]
    fn the_newest_frame_of_the_tier_stands_once_queried() {
        let mut frames = TriggerFrames::default();
        assert_eq!(frames.on_received(broadcast(btc(2, "99"))), Received::Taken);
        assert_eq!(first_oid(&frames), Err("querying"));
        frames.on_query(Ok(vec![btc(1, "98")]));
        assert_eq!(first_oid(&frames), Ok(2), "the broadcast's newer frame kept over the query's older");
        frames.on_received(broadcast(btc(3, "97")));
        frames.on_received(broadcast(btc(1, "96")));
        frames.on_received(broadcast(frame("BTC", Tier::Near, 9, FlowContent::Unavailable("x".to_string()))));
        assert_eq!(first_oid(&frames), Ok(3));
        assert_eq!(frames.ready("ETH").err(), Some("no_frame"));
        frames.on_received(broadcast(frame("BTC", TRIGGER_TIER, 4, FlowContent::Unavailable("no mark".to_string()))));
        assert_eq!(first_oid(&frames), Err("unavailable"));
        let mut incomplete = ready("100", vec![stop(5, 1, "99")]);
        incomplete.triggers = TriggerStatus::Incomplete("truncated".to_string());
        frames.on_received(broadcast(frame("BTC", TRIGGER_TIER, 5, FlowContent::Ready(Box::new(incomplete)))));
        assert_eq!(first_oid(&frames), Err("incomplete"));
    }

    /// A lag calls for a full query and leaves every coin unknown until it answers - an answer of the same `seq`s ends
    /// the wait too (review 000642 r2) -; a refused query stops them.
    #[test]
    fn a_lag_waits_for_a_full_query() {
        let mut frames = TriggerFrames::default();
        frames.on_query(Ok(vec![btc(1, "99")]));
        assert_eq!(frames.on_received(Err(RecvError::Lagged(3))), Received::QueryDue);
        assert_eq!(first_oid(&frames), Err("querying"));
        frames.on_query(Ok(vec![btc(1, "99")]));
        assert_eq!(first_oid(&frames), Ok(1), "same seq: the wait ends");
        frames.on_received(Err(RecvError::Lagged(1)));
        frames.on_query(Err("the forced flow is gone".to_string()));
        assert_eq!(first_oid(&frames), Err("stopped"));
        assert_eq!(frames.on_received(Err(RecvError::Lagged(1))), Received::Taken, "stopped: no query");
    }

    /// The forced flow withdrawn (the broadcast still open: other frames go on) or the broadcast closed: every coin's
    /// frames dropped until restart, an answer or a frame coming later taken by none (review 000642 r1 P2-3).
    #[test]
    fn a_withdrawal_ends_the_frames_until_restart() {
        for end in [Ok(Arc::new(AccountsFrame::ForcedFlowWithdrawn { reason: "thread gone".to_string() })), Err(RecvError::Closed)] {
            let mut frames = TriggerFrames::default();
            frames.on_query(Ok(vec![btc(1, "99")]));
            assert_eq!(frames.on_received(end), Received::Ended);
            assert_eq!(first_oid(&frames), Err("stopped"));
            frames.on_query(Ok(vec![btc(2, "99")]));
            frames.on_received(broadcast(btc(3, "99")));
            assert_eq!(first_oid(&frames), Err("stopped"));
        }
        let mut frames = TriggerFrames::default();
        frames.on_query(Ok(vec![btc(1, "99")]));
        let other = AccountsFrame::WithdrawCoin { coin: "BTC".to_string(), reason: "delisted".to_string() };
        assert_eq!(frames.on_received(Ok(Arc::new(other))), Received::Taken, "another frame passes");
        assert_eq!(first_oid(&frames), Ok(1));
    }
}
