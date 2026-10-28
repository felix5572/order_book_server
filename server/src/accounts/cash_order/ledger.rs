//! A send settles before the ordinary signed-bundle fills it may fund once its execution order is proven:
//! - a successful delayed system send. Client 6911b792: `SystemSendAssetAction` tag 0xcc selects D at 0x42ab551;
//!   positive D*scale queues it, and block 0x297e530 drains that queue before signed bundles. EUR H1194923131
//!   confirms the cash effect.
//! - a successful signed `sendAsset` ranked before every fill of both its accounts in the block. TRUMP H1200570658:
//!   the receiver's 1x isolated open, a bundle after the send, spent it; applied after the fill, the send left the
//!   isolated transfer capped at the pre-send cash and the replayed row insolvent (`forced_flow_eval`, 10-28 17:12).
//!
//! Attempts alone never credit money: match the successful ledger.
use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};
use crate::{accounts::{actions::{BlockActions, RankedSend}, marks::{MarkInputs, actions::Rank,
    execution::fill_order::{Placement, takers}, system::SystemAction}, misc::{Balance, LedgerDelta, MiscEvent, SpotToMainSend}},
    types::node_data::{Batch, NodeDataFill}};

#[derive(Default)]
pub(crate) struct PreBundleLedger {
    /// The moved sends' (sender, nonce).
    sends: FxHashSet<(Address, u64)>,
}

/// When a confirmed send's cash moved within its block.
#[derive(Clone, Copy)]
enum Timing {
    /// A delayed system send: before every signed bundle.
    BeforeBundles,
    /// A signed `sendAsset`: at its rank.
    At(Rank),
}

impl PreBundleLedger {
    pub(crate) fn of(actions: &BlockActions, fills: &[Batch<NodeDataFill>], misc: &[Batch<MiscEvent>],
        inputs: &MarkInputs, delayed: bool) -> Self {
        if fills.is_empty() { return Self::default(); }
        let requests: Vec<_> = if delayed {
            inputs.system_actions.iter().flat_map(Batch::events_ref).filter_map(SystemAction::spot_to_main_usdc).collect()
        } else {
            Vec::new()
        };
        if requests.is_empty() && actions.sends.is_empty() { return Self::default(); }
        let Ok(mark_actions) = &inputs.mark_actions else { return Self::default(); };
        let triggered = inputs.order_statuses.iter().flat_map(Batch::events_ref)
            .filter(|row| row.status == "triggered").map(|row| row.order.oid).collect();
        let placements = takers(&inputs.fills, mark_actions, &triggered, &[]);
        let mut ledgers = FxHashMap::<Address, usize>::default();
        for event in misc.iter().flat_map(Batch::events_ref) {
            match event {
                MiscEvent::Ledger { users, .. } => for user in users { *ledgers.entry(*user).or_default() += 1; },
                MiscEvent::Unknown(_) => return Self::default(),
                _ => {},
            }
        }
        let mut result = Self::default();
        for event in misc.iter().flat_map(Batch::events_ref) {
            let Some(send) = confirmed_send(event) else { continue; };
            let Some(timing) = timing(&send, &requests, &actions.sends) else { continue; };
            let participants = [send.user, send.destination];
            if participants.iter().any(|user| ledgers.get(user) != Some(&1))
                || actions.actions.iter().any(|(_, user)| participants.contains(user)) { continue; }
            let mut has_fill = false;
            let after_send = fills.iter().flat_map(Batch::events_ref).filter(|NodeDataFill(user, _)| participants.contains(user)).all(|NodeDataFill(_, fill)| {
                has_fill = true;
                match (placements.get(&(fill.coin.as_str(), fill.tid)), timing) {
                    (Some(Some(Placement::Action(_))), Timing::BeforeBundles) => true,
                    (Some(Some(Placement::Action(rank))), Timing::At(send_rank)) => *rank > send_rank,
                    _ => false,
                }
            });
            // Other delayed, TWAP, trigger or unidentified executions, or a fill before a signed send, may surround
            // this send. Keep their old estimate.
            if has_fill && after_send { result.sends.insert((send.user, send.nonce)); }
        }
        result
    }

    /// Move confirmed outcomes once, after funding but before actions/fills, and log that same order for reseeding.
    pub(crate) fn take(self, misc: &mut [Batch<MiscEvent>], trusted: bool) -> Vec<Batch<MiscEvent>> {
        if !trusted || self.sends.is_empty() { return Vec::new(); }
        let selected = |event: &MiscEvent| confirmed_send(event).is_some_and(|send| self.sends.contains(&(send.user, send.nonce)));
        let mut before = Vec::new();
        for batch in misc {
            if !batch.events_ref().iter().any(&selected) { continue; }
            let mut moved = batch.clone();
            moved.retain_events(&selected);
            batch.retain_events(|event| !selected(event));
            before.push(moved);
        }
        before
    }
}

/// The send's timing: a unique delayed system request with its sender and nonce, else a unique signed `sendAsset`
/// with them; either must name the ledger's every field. `None` without one.
fn timing(send: &SpotToMainSend, requests: &[SpotToMainSend], signed: &[RankedSend]) -> Option<Timing> {
    let mut delayed = requests.iter().filter(|request| (request.user, request.nonce) == (send.user, send.nonce));
    if let Some(request) = delayed.next() {
        return (request == send && delayed.next().is_none()).then_some(Timing::BeforeBundles);
    }
    let mut ranked = signed.iter().filter(|ranked| (ranked.send.user, ranked.send.nonce) == (send.user, send.nonce));
    let first = ranked.next()?;
    (first.send == *send && ranked.next().is_none()).then_some(Timing::At(first.rank))
}

fn confirmed_send(event: &MiscEvent) -> Option<SpotToMainSend> {
    let MiscEvent::Ledger { users, delta: LedgerDelta::Send { user, destination, source: Balance::Spot,
        destination_balance: Balance::Dex(dex), usdc_wei: Some(wei), usdc_fee_wei: 0, nonce: Some(nonce) } } = event else { return None; };
    if !dex.is_empty() || *wei <= 0 || user == destination || users.len() != 2 || !users.contains(user) || !users.contains(destination) { return None; }
    Some(SpotToMainSend { user: *user, destination: *destination, nonce: *nonce, wei: u64::try_from(*wei).ok()? })
}

#[cfg(test)]
pub(crate) mod tests;
