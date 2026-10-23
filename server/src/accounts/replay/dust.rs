//! A complete block's system spot-dust conversion, distinct from an ordinary order fill.
//!
//! H1193705508: 52 batches / 1628 recipients. The reserved 0xee..ee account sells the combined dust at
//! order-lot precision. Its net USDC proceeds are distributed at token-wei precision; the displayed
//! recipient px*sz omits both the unsold sub-lot remainder and the system trade's fee.
//! Client 6911b792: 0x467f7e0 sorts amounts descending, 0x3bdc3d0 truncates f64(qty/total*net),
//! 0x467fc68 adds the rounding remainder to the largest holder. Same bytes in running a67d59b7.
//! Only the measured single-fill, fully sold order-lot path is accepted. Equal largest holders with a
//! nonzero remainder remain unresolved: the node's large-vector equal-key ordering is not established.

use std::sync::{Arc, OnceLock};
use alloy::primitives::Address;
use rustc_hash::FxHashMap;
use crate::{order_book::Side, types::{Fill, node_data::NodeDataFill}};
use super::super::{checkpoint::{SpotPair, MAX_SPOT_DECIMALS}, size::parse_size_raw,
    usdc::{notional_wei, usdc_wei}};

pub(crate) const DUST_DIR: &str = "Spot Dust Conversion";
const SYSTEM: Address = Address::new([0xee; 20]);
const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const MAX_EXACT: i64 = 9_007_199_254_740_992;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Member { user: Address, quantity: String, start: String }

#[derive(Debug, Clone, PartialEq, Eq)]
struct Sale { price: String, quantity: String, start: String, fee: String }

#[derive(Debug, Default, PartialEq, Eq)]
struct Inputs { members: Vec<Member>, sales: Vec<Sale>, invalid: Option<String> }

#[derive(Debug)]
struct DustBatch {
    inputs: Inputs,
    indices: FxHashMap<Address, usize>,
    // Every recipient and delayed-seed replay shares these immutable inputs. Calculate the O(N)
    // allocation once, avoiding O(N²) work for a large batch; metadata is checked on every reuse.
    projection: OnceLock<(SpotPair, Result<Vec<i64>, String>)>,
}

impl PartialEq for DustBatch {
    fn eq(&self, other: &Self) -> bool { self.inputs == other.inputs }
}
impl Eq for DustBatch {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DustShare { batch: Arc<DustBatch>, index: usize }

impl DustShare {
    pub(crate) fn credit_wei(&self, pair: &SpotPair) -> Result<i64, String> {
        let (metadata, credits) = self.batch.projection.get_or_init(|| {
            (pair.clone(), self.batch.inputs.allocate(pair))
        });
        if metadata != pair { return Err("dust token metadata changed during replay".to_string()); }
        credits.as_ref().map(|credits| credits[self.index]).map_err(Clone::clone)
    }
}

/// Constructed from all fill lines of one released block, before any recipient is applied.
#[derive(Debug, Default)]
pub(crate) struct DustBlock { batches: FxHashMap<(String, u64), Arc<DustBatch>> }

impl DustBlock {
    pub(crate) fn of<'a>(fills: impl Iterator<Item = &'a NodeDataFill> + Clone) -> Self {
        let mut groups: FxHashMap<(String, u64), Inputs> = FxHashMap::default();
        for NodeDataFill(user, fill) in fills.clone().filter(|NodeDataFill(_, fill)| fill.dir == DUST_DIR) {
            let inputs = groups.entry((fill.coin.clone(), fill.oid)).or_default();
            if fill.side != Side::Ask || !fill.crossed || fill.tid != 0 || fill.hash != ZERO_HASH
                || fill.fee_token != "USDC" || usdc_wei(&fill.fee) != Ok(0)
                || *user == SYSTEM
            {
                inputs.invalid = Some("unverified dust recipient shape".to_string());
            }
            inputs.members.push(Member { user: *user, quantity: fill.sz.clone(), start: fill.start_position.clone() });
        }
        if groups.is_empty() { return Self::default(); }
        for NodeDataFill(_, fill) in fills.filter(|NodeDataFill(user, _)| *user == SYSTEM) {
            let Some(inputs) = groups.get_mut(&(fill.coin.clone(), fill.oid)) else { continue; };
            if fill.side != Side::Ask || !fill.crossed || fill.dir != "Sell"
                || fill.tid == 0 || fill.hash != ZERO_HASH || fill.fee_token != "USDC"
            {
                inputs.invalid = Some("unverified system dust sale shape".to_string());
            }
            inputs.sales.push(Sale { price: fill.px.clone(), quantity: fill.sz.clone(),
                start: fill.start_position.clone(), fee: fill.fee.clone() });
        }
        Self { batches: groups.into_iter().map(|(key, mut inputs)| {
            let mut indices = FxHashMap::default();
            for (index, member) in inputs.members.iter().enumerate() {
                if indices.insert(member.user, index).is_some() {
                    inputs.invalid = Some("duplicate dust recipient".to_string());
                }
            }
            (key, Arc::new(DustBatch { inputs, indices, projection: OnceLock::new() }))
        }).collect() }
    }

    pub(crate) fn share(&self, user: Address, fill: &Fill) -> Option<DustShare> {
        if fill.dir != DUST_DIR { return None; }
        let batch = self.batches.get(&(fill.coin.clone(), fill.oid))?;
        Some(DustShare { batch: batch.clone(), index: *batch.indices.get(&user)? })
    }
}

impl Inputs {
    fn allocate(&self, pair: &SpotPair) -> Result<Vec<i64>, String> {
        if let Some(error) = &self.invalid { return Err(error.clone()); }
        if pair.quote_token != 0 || pair.quote_wei_decimals != 8 {
            return Err("dust quote is not 8-decimal USDC".to_string());
        }
        let [sale] = self.sales.as_slice() else {
            return Err(format!("dust batch has {} system fills; expected the measured single-fill path", self.sales.len()));
        };
        let amounts = self.members.iter().map(|member| {
            let raw = parse_size_raw(&member.quantity, pair.base_wei_decimals)?;
            if raw <= 0 || raw > MAX_EXACT { return Err("dust amount outside exact positive range".to_string()); }
            if parse_size_raw(&member.start, pair.base_wei_decimals)? != raw {
                return Err("dust recipient quantity differs from starting balance".to_string());
            }
            Ok(raw)
        }).collect::<Result<Vec<_>, String>>()?;
        let total = amounts.iter().try_fold(0_i64, |sum, value| sum.checked_add(*value))
            .filter(|total| *total > 0 && *total <= MAX_EXACT)
            .ok_or_else(|| "dust total outside exact positive range".to_string())?;
        if parse_size_raw(&sale.start, pair.base_wei_decimals)? != total {
            return Err("dust recipients do not sum to the system starting balance".to_string());
        }
        let lot_scale = pair.base_wei_decimals.checked_sub(pair.size_decimals)
            .and_then(|power| 10_i64.checked_pow(power)).ok_or_else(|| "invalid dust token/lot precision".to_string())?;
        let sold = parse_size_raw(&sale.quantity, pair.base_wei_decimals)?;
        if sold <= 0 || sold != total / lot_scale * lot_scale {
            return Err("system sale did not fully sell the combined order-lot quantity".to_string());
        }
        let price_decimals = MAX_SPOT_DECIMALS.checked_sub(pair.size_decimals)
            .ok_or_else(|| "invalid dust price precision".to_string())?;
        let gross = notional_wei(&sale.price, &sale.quantity, pair.size_decimals, price_decimals)?;
        let fee = usdc_wei(&sale.fee)?;
        let net = gross.checked_sub(fee).filter(|net| fee >= 0 && *net >= 0 && *net <= MAX_EXACT)
            .ok_or_else(|| "dust proceeds outside exact nonnegative range".to_string())?;
        proportional_credits(&amounts, total, net)
    }
}

#[expect(clippy::cast_precision_loss, clippy::cast_possible_truncation, reason = "client uses f64 then truncation; inputs bounded by 2^53")]
fn proportional_credits(amounts: &[i64], total: i64, net: i64) -> Result<Vec<i64>, String> {
    let mut credits: Vec<_> = amounts.iter().map(|amount| ((*amount as f64 / total as f64) * net as f64) as i64).collect();
    let allocated: i64 = credits.iter().sum();
    let remainder = net.checked_sub(allocated).filter(|remaining| *remaining >= 0)
        .ok_or_else(|| "dust rounded allocation exceeded proceeds".to_string())?;
    if remainder == 0 { return Ok(credits); }
    let (index, largest) = amounts.iter().enumerate().max_by_key(|(_, amount)| *amount)
        .ok_or_else(|| "empty dust batch".to_string())?;
    if amounts.iter().filter(|amount| *amount == largest).count() > 1 {
        return Err("dust remainder recipient tied; client tie ordering not verified".to_string());
    }
    credits[index] += remainder;
    Ok(credits)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use serde_json::Value;

    type GoldenBatch = (SpotPair, Vec<NodeDataFill>, Vec<(Address, i64)>);

    pub(crate) fn goldens() -> Vec<GoldenBatch> {
        let data: Value = serde_json::from_str(include_str!("../spot_dust_allocation_goldens.json")).unwrap();
        data["groups"].as_array().unwrap().iter().map(|group| {
            let pair = SpotPair { size_decimals: u32::try_from(group["base_size_decimals"].as_u64().unwrap()).unwrap(),
                base_wei_decimals: u32::try_from(group["base_wei_decimals"].as_u64().unwrap()).unwrap(),
                quote_token: u32::try_from(group["quote_token"].as_u64().unwrap()).unwrap(), quote_wei_decimals: 8 };
            let mut fills = vec![serde_json::from_value::<NodeDataFill>(group["sale"].clone()).unwrap()];
            let mut expected = Vec::new();
            for member in group["members"].as_array().unwrap() {
                let user: Address = serde_json::from_value(member[0].clone()).unwrap();
                let mut wire = group["recipient_template"].clone();
                wire["sz"] = member[1].clone();
                wire["startPosition"] = member[1].clone();
                fills.push(NodeDataFill(user, serde_json::from_value(wire).unwrap()));
                expected.push((user, member[2].as_i64().unwrap()));
            }
            (pair, fills, expected)
        }).collect()
    }

    #[test]
    fn real_batches_match_all_1628_original_client_allocations() {
        let cases = goldens();
        assert_eq!(cases.len(), 52);
        let mut count = 0;
        for (pair, fills, expected) in cases {
            let block = DustBlock::of(fills.iter());
            for ((user, credit), NodeDataFill(actual_user, fill)) in expected.iter().zip(&fills[1..]) {
                assert_eq!(user, actual_user);
                let share = block.share(*user, fill).unwrap();
                assert_eq!(share.credit_wei(&pair), Ok(*credit), "{} {user}", fill.coin);
                assert_eq!(share.clone().credit_wei(&pair), Ok(*credit), "shared projection replay");
                count += 1;
            }
        }
        assert_eq!(count, 1628);
    }

    #[test]
    fn incomplete_and_unverified_batches_cannot_create_credible_cash() {
        let (pair, fills, _) = goldens().remove(0);
        for change in 0..8 {
            let mut altered = fills.clone();
            match change {
                0 => { altered.remove(0); },
                1 => { altered.pop(); },
                2 => { altered.push(altered[1].clone()); },
                3 => { altered[0].1.sz = "23.0".to_string(); },
                4 => { altered[0].1.fee_token = "PURR".to_string(); },
                5 => { altered[0].1.fee = "-1.0".to_string(); },
                6 => { altered.push(altered[0].clone()); },
                7 => { altered[1].1.side = Side::Bid; },
                _ => unreachable!(),
            }
            let block = DustBlock::of(altered.iter());
            let NodeDataFill(user, fill) = &fills[1];
            assert!(block.share(*user, fill).unwrap().credit_wei(&pair).is_err(), "case {change}");
        }
    }

    #[test]
    fn allocation_is_per_coin_order_and_metadata_errors_are_not_hidden() {
        let mut cases = goldens();
        let (pair, mut first, expected) = cases.remove(0);
        let (_, mut second, _) = cases.remove(0);
        // Same oid in another coin cannot contaminate the batch.
        let oid = first[0].1.oid;
        for row in &mut second { row.1.oid = oid; }
        first.extend(second);
        let block = DustBlock::of(first.iter());
        let share = block.share(first[1].0, &first[1].1).unwrap();
        assert_eq!(share.credit_wei(&pair), Ok(expected[0].1));
        let mut changed = pair.clone();
        changed.base_wei_decimals += 1;
        assert!(share.credit_wei(&changed).unwrap_err().contains("metadata changed"));
        first.reverse();
        let reversed = DustBlock::of(first.iter());
        let row = first.iter().find(|row| row.0 == expected[0].0 && row.1.coin == "PURR/USDC").unwrap();
        assert_eq!(reversed.share(row.0, &row.1).unwrap().credit_wei(&pair), Ok(expected[0].1));
    }

    #[test]
    fn unknown_largest_tie_is_withdrawn_only_when_a_remainder_exists() {
        assert_eq!(proportional_credits(&[1, 1], 2, 4), Ok(vec![2, 2]));
        assert!(proportional_credits(&[1, 1], 2, 3).unwrap_err().contains("tied"));
        assert_eq!(proportional_credits(&[3, 1], 4, 3), Ok(vec![3, 0]));
        assert_eq!(proportional_credits(&[3, 1], 4, 0), Ok(vec![0, 0]));
    }
}
