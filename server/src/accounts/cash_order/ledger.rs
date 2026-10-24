//! A successful delayed system send settles before ordinary signed-bundle fills.
//! Client 6911b792: `SystemSendAssetAction` tag 0xcc selects D at 0x42ab551; positive D*scale
//! queues it, and block 0x297e530 drains that queue before signed bundles. EUR H1194923131
//! confirms the cash effect. Attempts alone never credit money: match the successful ledger.
use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};
use crate::{accounts::{actions::BlockActions, marks::{MarkInputs, execution::fill_order::{Placement, takers},
    system::DelayedSend}, misc::{Balance, LedgerDelta, MiscEvent}}, types::node_data::{Batch, NodeDataFill}};

#[derive(Default)]
pub(crate) struct PreBundleLedger {
    sends: FxHashSet<(Address, u64)>,
}

impl PreBundleLedger {
    pub(crate) fn of(actions: &BlockActions, fills: &[Batch<NodeDataFill>], misc: &[Batch<MiscEvent>],
        inputs: &MarkInputs, delayed: bool) -> Self {
        if !delayed || fills.is_empty() { return Self::default(); }
        let requests: Vec<_> = inputs.system_actions.iter().flat_map(Batch::events_ref)
            .filter_map(super::super::marks::system::SystemAction::spot_to_main_usdc).collect();
        if requests.is_empty() { return Self::default(); }
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
            let mut candidates = requests.iter().filter(|request| (request.user, request.nonce) == (send.user, send.nonce));
            if candidates.next() != Some(&send) || candidates.next().is_some() { continue; }
            let participants = [send.user, send.destination];
            if participants.iter().any(|user| ledgers.get(user) != Some(&1))
                || actions.actions.iter().any(|(_, user)| participants.contains(user)) { continue; }
            let mut has_fill = false;
            let ordinary = fills.iter().flat_map(Batch::events_ref).filter(|NodeDataFill(user, _)| participants.contains(user)).all(|NodeDataFill(_, fill)| {
                has_fill = true;
                matches!(placements.get(&(fill.coin.as_str(), fill.tid)), Some(Some(Placement::Action(_))))
            });
            // Other delayed, TWAP, trigger or unidentified executions may surround this send. Keep their old estimate.
            if has_fill && ordinary { result.sends.insert((send.user, send.nonce)); }
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

fn confirmed_send(event: &MiscEvent) -> Option<DelayedSend> {
    let MiscEvent::Ledger { users, delta: LedgerDelta::Send { user, destination, source: Balance::Spot,
        destination_balance: Balance::Dex(dex), usdc_wei: Some(wei), usdc_fee_wei: 0, nonce: Some(nonce) } } = event else { return None; };
    if !dex.is_empty() || *wei <= 0 || user == destination || users.len() != 2 || !users.contains(user) || !users.contains(destination) { return None; }
    Some(DelayedSend { user: *user, destination: *destination, nonce: *nonce, wei: u64::try_from(*wei).ok()? })
}

#[cfg(test)]
pub(crate) mod tests;
