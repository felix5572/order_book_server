use std::{fs::File, io::{BufRead, BufReader}};
use serde_json::{Value, json};
use crate::{accounts::{checkpoint::{parse_checkpoint_bytes, tests::fixture}, marks::{seed::MarkSeed, MarkState, BlockInputs, MarksBlockReport, actions::MarkActions, execution::{BlockFill, BlockStatuses}, book::BookDiff}, replica::ReplicaLine}, types::node_data::{NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus}};

fn seed(index: usize) -> MarkSeed {
    let golden: Value = serde_json::from_str(include_str!("sand_goldens.json")).unwrap();
    let g = &golden["states"][index];
    let mut v: Value = rmp_serde::from_slice(&fixture()).unwrap();
    let ex = &mut v["exchange"];
    ex["msc"] = g["msc"].clone(); ex["begin_block_logic_guard"] = g["outer"].clone();
    ex["post_only_until_time"] = g["post_time"].clone(); ex["post_only_until_height"] = g["post_height"].clone();
    ex["locus"]["ctx"] = g["ctx"].clone();
    let mut main = ex["locus"]["cls"][0].clone();
    main["meta"] = g["meta"].clone(); main["oracle"] = g["oracle"].clone(); main["lnsgt"] = g["lnsgt"].clone();
    main["halted_assets"] = json!([]); main["user_states"] = json!({"user_to_state":[],"open_order_tracker":{"user_to_data":[]}});
    ex["locus"]["cls"] = json!([main]); ex["perp_dexs"] = json!([{"books":[g["book"]]}]);
    MarkSeed::from_checkpoint(parse_checkpoint_bytes(&rmp_serde::to_vec_named(&v).unwrap()).unwrap()).unwrap()
}

// This projection has only SAND book requests and price input slot 156. Preserve every
// receipt/rank and order, changing only that asset's index in the one-coin checkpoint.
fn remap(v: &mut Value) {
    match v {
        Value::Array(xs) => for x in xs { remap(x); },
        Value::Object(m) => {
            if m.get("type").and_then(Value::as_str) == Some("SetGlobalAction") {
                m.insert("pxs".into(), json!([m["pxs"][156]]));
            }
            if m.get("a").and_then(Value::as_u64) == Some(156) { m.insert("a".into(), json!(0)); }
            for x in m.values_mut() { remap(x); }
        },
        _ => {},
    }
}

#[test]
#[ignore = "requires archived 10,000-block SAND stream projection in SAND_REPLAY_INPUT"]
fn sand_historical_production_replay() {
    let path = std::env::var("SAND_REPLAY_INPUT").expect("SAND_REPLAY_INPUT");
    let mut state = MarkState::seeded(seed(0));
    let mut old_seed = seed(0); old_seed.native.refresh.stale_after_ns = None;
    let mut control = MarkState::seeded(old_seed);
    let mut count = 0; let mut focal = 0;
    for line in BufReader::new(File::open(path).unwrap()).lines() {
        let mut v: Value = serde_json::from_str(&line.unwrap()).unwrap(); remap(&mut v["replica"]);
        let text = v["replica"].to_string();
        let actions = MarkActions::of(&ReplicaLine::parse(&text).unwrap()).unwrap();
        let height = actions.height;
        let diffs: Vec<NodeDataOrderDiff> = serde_json::from_value(v["diffs"].clone()).unwrap();
        let fills: Vec<NodeDataFill> = serde_json::from_value(v["fills"].clone()).unwrap();
        let statuses: Vec<NodeDataOrderStatus> = serde_json::from_value(v["statuses"].clone()).unwrap();
        let inputs = BlockInputs { actions: Some(actions), diffs: diffs.iter().map(|d| BookDiff::of(d).unwrap()).collect(), fills: fills.iter().map(BlockFill::of).collect(), statuses: BlockStatuses::of(statuses).unwrap(), ..BlockInputs::default() };
        let mut report = MarksBlockReport::default(); state.apply(height, &inputs, &mut report);
        assert!(report.errors.is_empty(), "{height}: {:?}", report.errors);
        let mut old_report = MarksBlockReport::default(); control.apply(height, &inputs, &mut old_report);
        assert!(old_report.errors.is_empty(), "{height}: {:?}", old_report.errors);
        if height == 1197481585 {
            for f in &inputs.fills {
                let p = report.fill_prices.get(height, "SAND", f.tid).unwrap();
                assert_eq!((p.min_px_raw, p.max_px_raw), (47053, 47053));
                let old = old_report.fill_prices.get(height, "SAND", f.tid).unwrap();
                assert_eq!((old.min_px_raw, old.max_px_raw), (47046, 47046)); focal += 1;
            }
        }
        count += 1;
    }
    assert_eq!(count, 10000); assert!(focal > 0);
    let expected = seed(1);
    let prices = state.prices.check_against(&expected.native);
    assert!(prices.unsupported.is_empty(), "{prices:?}");
    assert_eq!(prices.fields.len(), 7);
    assert!(prices.fields.iter().all(|(_, equal, differ)| (*equal, *differ) == (1, 0)), "{prices:?}");
    // Later explicit updates erase the endpoint difference. The focal fill assertion
    // above is the negative control: checking only the checkpoint would miss this bug.
    assert!(control.prices.check_against(&expected.native).samples.is_empty());
    let books = state.books.check_against(&expected.native);
    assert_eq!(books.books_equal, 1); assert!(books.samples.is_empty(), "{books:?}");
    println!("SAND production replay: {count} blocks, 7/7 fields exact, book exact, {focal} focal fill legs use 0.047053");
}

#[test]
fn automatic_refresh_precedes_current_fills_and_is_replayable_from_checkpoint() {
    use crate::accounts::marks::seed::RestingOrder;
    use crate::order_book::Side;
    let mut seeded = seed(0);
    // A controlled book moved above the submitted mark. The selector must use this
    // previous-block book and advance before the first fill, even without an explicit SGA.
    seeded.native.assets[0].orders = vec![
        RestingOrder { oid: 1, side: Side::Bid, limit_px: 50000, remaining: 10 },
        RestingOrder { oid: 2, side: Side::Ask, limit_px: 50002, remaining: 10 },
    ];
    seeded.native.assets[0].last_trade_px = 50001;
    let at = seeded.native.refresh.last_explicit_ns + 21_000_000_000;
    let make = |t| BlockInputs {
        actions: Some(MarkActions { height: 1197480001, time_ns: t, actions: vec![] }),
        fills: vec![BlockFill { coin: "SAND".into(), px: "0.050001".into(), sz: "1".into(), crossed: true, oid: 3, tid: 10, action_position: None, twap: false, liquidation_mark_px: None }],
        collateral_coins: vec!["SAND".into()], ..BlockInputs::default()
    };
    let mut state = MarkState::seeded(seeded.clone());
    let mut reloaded = MarkState::seeded(seeded);
    let mut report = MarksBlockReport::default(); state.apply(1197480001, &make(at), &mut report);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let mark = report.fill_prices.get(1197480001, "SAND", 10).unwrap();
    assert_eq!((mark.min_px_raw, mark.max_px_raw), (47138, 47138));
    let collateral = report.fill_prices.collateral(1197480001).unwrap();
    assert_eq!(collateral["SAND"], mark);
    let mut replay = MarksBlockReport::default(); reloaded.apply(1197480001, &make(at), &mut replay);
    assert_eq!(report, replay);
    let prices = state.prices.price_states().next().unwrap().1;
    let mut repeat = MarksBlockReport::default(); state.apply(1197480002, &make(at), &mut repeat);
    assert_eq!(state.prices.price_states().next().unwrap().1.emas, prices.emas, "same bucket cannot update EMAs twice");
}
