//! The `extLiquidationCandidates` subscriptions of a WS connection (design 000443): each projects the owner's candidate
//! frames onto its coin and distance (`CandidateProjection`); a missed frame or the feed withdrawn ends it.

use std::collections::HashMap;

use log::error;

use super::Outbound;
use crate::{
    listeners::order_book::BookTrust,
    accounts::{
        candidates::CandidateChanges,
        candidates_view::{CandidateFilter, CandidateProjection},
        runtime::AccountsHandle,
    },
    types::subscription::{ClientMessage, EXT_LIQUIDATION_CANDIDATES, ServerResponse, Subscription, SubscriptionManager},
};

pub(super) struct CandidateSubscriptions {
    projections: HashMap<Subscription, CandidateProjection>,
    /// The book the walks were read from: every frame's walks are published as of its state when the frame goes out.
    book: BookTrust,
}

impl CandidateSubscriptions {
    pub(super) fn new(book: BookTrust) -> Self {
        Self { projections: HashMap::new(), book }
    }

    /// The book's (trusted, withheld periods) now.
    fn book_now(&self) -> (bool, u64) {
        (self.book.is_trusted(), self.book.withheld_periods())
    }

    /// Subscribe / unsubscribe; `filter` is the validated subscription's. Returns false when the connection must be
    /// dropped.
    pub(super) async fn on_client_message(
        &mut self,
        accounts: &AccountsHandle,
        outbound: &Outbound,
        manager: &mut SubscriptionManager,
        client_message: ClientMessage,
        subscription: Subscription,
        filter: CandidateFilter,
    ) -> bool {
        // display only, as for the other subscriptions
        let sub = serde_json::to_string(&subscription).unwrap_or_default();
        if matches!(client_message, ClientMessage::Unsubscribe { .. }) {
            if !manager.unsubscribe(subscription.clone()) {
                return outbound.send_message(ServerResponse::Error(format!("Already unsubscribed: {sub}"))).await;
            }
            self.projections.remove(&subscription);
            return outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await;
        }
        match manager.subscribe(subscription.clone()) {
            Ok(true) => {}
            Ok(false) => return outbound.send_message(ServerResponse::Error(format!("Already subscribed: {sub}"))).await,
            Err(err) => return outbound.send_message(ServerResponse::Error(format!("Rejected subscription: {err}"))).await,
        }
        // the frames receiver was joined at connect time, so every frame after this snapshot reaches the projection
        let snapshot = match accounts.liquidation_candidates().await {
            Ok(snapshot) => snapshot,
            Err(reason) => {
                manager.unsubscribe(subscription);
                let message = format!("Unable to grab {EXT_LIQUIDATION_CANDIDATES} snapshot: {reason}");
                return outbound.send_message(ServerResponse::Error(message)).await;
            }
        };
        if !outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        let (projection, frame) = CandidateProjection::subscribe(filter, &snapshot, Some(self.book_now()));
        self.projections.insert(subscription, projection);
        outbound.send_frame(frame.to_wire()).await
    }

    /// One owner frame for every subscription. Returns false when the connection must be dropped.
    pub(super) async fn on_changes(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager, changes: &CandidateChanges) -> bool {
        let mut gaps = Vec::new();
        let book = Some(self.book_now());
        for (subscription, projection) in &mut self.projections {
            match projection.apply(changes, book) {
                Ok(Some(frame)) => {
                    if !outbound.send_frame(frame.to_wire()).await {
                        return false;
                    }
                }
                Ok(None) => {}
                Err(reason) => gaps.push((subscription.clone(), reason)),
            }
        }
        for (subscription, reason) in gaps {
            error!("{EXT_LIQUIDATION_CANDIDATES}: {reason}; withdrawing");
            if !self.withdraw(outbound, manager, &subscription, &reason).await {
                return false;
            }
        }
        true
    }

    /// Ends every subscription. Returns false when the connection must be dropped.
    pub(super) async fn withdraw_all(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager, reason: &str) -> bool {
        let subscriptions: Vec<Subscription> = self.projections.keys().cloned().collect();
        for subscription in subscriptions {
            if !self.withdraw(outbound, manager, &subscription, reason).await {
                return false;
            }
        }
        true
    }

    async fn withdraw(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager, subscription: &Subscription, reason: &str) -> bool {
        let Some(projection) = self.projections.remove(subscription) else { return true };
        manager.unsubscribe(subscription.clone());
        outbound.send_frame(projection.withdrawn(reason).to_wire()).await
    }
}
