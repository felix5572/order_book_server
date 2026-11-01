//! Batch 2m-2 (hyperliquid-trade design 000652 r1 section 3, r2 P2-3): the account each signer acts for, learned from
//! the block log. A signed action without a vault runs for its signer's account - the signer itself, or the account
//! that approved it as its API wallet -, which the action's response names (`user`; research
//! `mempool_pending_book_20261031`, bm 10-31 20:15, one minute: 24,951 order actions without a vault, 1,196 signers,
//! each acting for one account). Only an action without a vault teaches it. A signer seen for two accounts is of none
//! until it expires. Each signer is kept `SIGNER_TTL_MS` of block time past its last sighting, at most `MAX_SIGNERS` of
//! them, the longest unseen going first.

use std::collections::{BTreeSet, hash_map::Entry};

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

/// How long a signer is kept unseen, block time: an API wallet lives on.
const SIGNER_TTL_MS: u64 = 24 * 3_600_000;

/// Signers kept at most (bm 10-31: ~1,200 a minute act without a vault).
const MAX_SIGNERS: usize = 200_000;

/// What the block log taught of a signer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Learned {
    Account(Address),
    /// Seen for two accounts: of none.
    Conflicting,
}

/// The signers' accounts and when each was last seen, block time ms.
#[derive(Debug, Default)]
pub(crate) struct SignerAccounts {
    by_signer: FxHashMap<Address, (Learned, u64)>,
    /// Of `by_signer`, those conflicting.
    conflicting: usize,
    /// (last seen ms, signer) of each signer kept: the order they go in (review 000656 r1 P2-1).
    by_last_seen: BTreeSet<(u64, Address)>,
}

impl SignerAccounts {
    /// A block at `seen_ms` ran an action of `signer` for `account`. Whether the signer turned conflicting with it.
    pub(crate) fn learn(&mut self, seen_ms: u64, signer: Address, account: Address) -> bool {
        match self.by_signer.entry(signer) {
            Entry::Occupied(mut entry) => {
                let (learned, last_ms) = entry.get_mut();
                let turns = matches!(*learned, Learned::Account(known) if known != account);
                if turns {
                    *learned = Learned::Conflicting;
                    self.conflicting += 1;
                }
                if seen_ms > *last_ms {
                    self.by_last_seen.remove(&(*last_ms, signer));
                    self.by_last_seen.insert((seen_ms, signer));
                    *last_ms = seen_ms;
                }
                turns
            }
            Entry::Vacant(entry) => {
                entry.insert((Learned::Account(account), seen_ms));
                self.by_last_seen.insert((seen_ms, signer));
                false
            }
        }
    }

    /// The signers unseen since `SIGNER_TTL_MS` before `now_ms` forgotten, then the longest unseen past `MAX_SIGNERS`:
    /// before a block's actions are learned, so an expired signer is learned anew, and after. Those forgotten, into
    /// `forgotten`: what was learned of them no longer stands (review 000656 r2 P2).
    pub(crate) fn evict(&mut self, now_ms: u64, forgotten: &mut FxHashSet<Address>) {
        let cutoff_ms = now_ms.saturating_sub(SIGNER_TTL_MS);
        while let Some(&(last_ms, signer)) = self.by_last_seen.first() {
            if last_ms >= cutoff_ms && self.by_signer.len() <= MAX_SIGNERS {
                return;
            }
            self.by_last_seen.pop_first();
            if let Some((Learned::Conflicting, _)) = self.by_signer.remove(&signer) {
                self.conflicting -= 1;
            }
            forgotten.insert(signer);
        }
    }

    /// The account `signer` acts for, or why it is not known.
    pub(crate) fn account(&self, signer: Address) -> Result<Address, &'static str> {
        match self.by_signer.get(&signer) {
            Some((Learned::Account(account), _)) => Ok(*account),
            Some((Learned::Conflicting, _)) => Err("signer_conflicting"),
            None => Err("signer_unseen"),
        }
    }

    /// The signers kept, (of one account, conflicting).
    pub(crate) fn counts(&self) -> (usize, usize) {
        (self.by_signer.len() - self.conflicting, self.conflicting)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIGNER: Address = Address::repeat_byte(0x51);
    const ALICE: Address = Address::repeat_byte(0xa1);
    const BOB: Address = Address::repeat_byte(0xb0);

    /// A signer of one account tells it; seen for a second account it tells none, from then on - turned so once.
    #[test]
    fn a_signer_of_two_accounts_tells_none() {
        let mut accounts = SignerAccounts::default();
        assert_eq!(accounts.account(SIGNER), Err("signer_unseen"));
        assert!(!accounts.learn(1_000, SIGNER, ALICE));
        assert!(!accounts.learn(2_000, SIGNER, ALICE));
        assert_eq!(accounts.account(SIGNER), Ok(ALICE));
        assert!(accounts.learn(3_000, SIGNER, BOB));
        assert!(!accounts.learn(4_000, SIGNER, ALICE));
        assert_eq!((accounts.account(SIGNER), accounts.counts()), (Err("signer_conflicting"), (0, 1)));
    }

    /// Review 000656 r1 P2-1: each signer is kept `SIGNER_TTL_MS` past its own last sighting, whatever order they were
    /// first seen and renewed in: B last seen at 12 h, A at 23 h; past 36 h B goes, A stays.
    #[test]
    fn a_signer_is_kept_its_ttl_past_its_last_sighting() {
        let hour = 3_600_000;
        let mut accounts = SignerAccounts::default();
        accounts.learn(0, ALICE, ALICE);
        accounts.learn(0, BOB, BOB);
        accounts.learn(12 * hour, BOB, BOB);
        accounts.learn(23 * hour, ALICE, ALICE);
        accounts.evict(24 * hour, &mut FxHashSet::default());
        assert_eq!((accounts.account(ALICE), accounts.account(BOB)), (Ok(ALICE), Ok(BOB)));
        let mut forgotten = FxHashSet::default();
        accounts.evict(36 * hour + 1, &mut forgotten);
        assert_eq!((accounts.account(ALICE), accounts.account(BOB), forgotten), (Ok(ALICE), Err("signer_unseen"), FxHashSet::from_iter([BOB])));
        accounts.evict(47 * hour + 1, &mut FxHashSet::default());
        assert_eq!(accounts.counts(), (0, 0));
    }

    /// Review 000656 r1 P2-1: a signer expired before its next block is learned anew there, not taken to conflict.
    #[test]
    fn an_expired_signer_is_learned_anew() {
        let mut accounts = SignerAccounts::default();
        accounts.learn(0, SIGNER, ALICE);
        let later = SIGNER_TTL_MS + 3_600_000;
        accounts.evict(later, &mut FxHashSet::default());
        assert!(!accounts.learn(later, SIGNER, BOB));
        assert_eq!(accounts.account(SIGNER), Ok(BOB));
    }

    /// Past `MAX_SIGNERS`, the longest unseen goes first: the first signer, seen again last, stays; one renewed in the
    /// middle stays past the second, never renewed.
    #[test]
    fn past_the_cap_the_longest_unseen_goes() {
        let signer = |index: usize| Address::from_word(alloy::primitives::B256::from(alloy::primitives::U256::from(index)));
        let ms = |index: usize| u64::try_from(index).unwrap();
        let mut accounts = SignerAccounts::default();
        for index in 1..=MAX_SIGNERS {
            accounts.learn(ms(index), signer(index), ALICE);
            if index == MAX_SIGNERS / 2 {
                accounts.learn(ms(index), signer(3), ALICE);
            }
        }
        accounts.learn(ms(MAX_SIGNERS) + 1, signer(1), ALICE);
        accounts.learn(ms(MAX_SIGNERS) + 2, signer(MAX_SIGNERS + 1), ALICE);
        let mut forgotten = FxHashSet::default();
        accounts.evict(ms(MAX_SIGNERS) + 2, &mut forgotten);
        let kept = [1, 2, 3, 4].map(|index| accounts.account(signer(index)).is_ok());
        assert_eq!((accounts.counts().0, kept, forgotten), (MAX_SIGNERS, [true, false, true, true], FxHashSet::from_iter([signer(2)])));
    }
}
