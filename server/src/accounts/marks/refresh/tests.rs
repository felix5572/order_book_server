use super::*;
use crate::accounts::{checkpoint::{parse_checkpoint_bytes, tests::fixture}, marks::seed::MarkSeed};

fn wire() -> WireRefreshSeed {
    WireRefreshSeed {
        config: Some(serde_json::from_value(serde_json::json!({
            "sgd": 20.0, "sog": {"last_bucket": 0, "bucket_millis": 3000},
            "mou": {"u": [[[0, 100]], [[0, 101]], []]}
        })).unwrap()),
        outer: Some(BucketGuard { last_bucket: 0, bucket_millis: 1000 }),
        post_only_until_time: Some("1970-01-01T00:00:00".into()),
        post_only_until_height: Some(0), hardfork_height: Some(0),
        last_explicit_time: Some("1970-01-01T00:00:00.500000000".into()),
    }
}

#[test]
fn stale_deadline_runs_only_on_a_new_outer_and_inner_bucket() {
    let mut s = RefreshState::from_wire(wire(), 1).unwrap();
    assert!(!s.due(6000, 20_400_000_000));
    assert!(!s.due(6001, 20_500_000_000), "deadline alone cannot bypass begin-block guard");
    assert!(s.due(6002, 21_000_000_000));
    assert!(!s.due(6003, 21_100_000_000));
    assert!(!s.due(6004, 22_000_000_000));
    assert!(s.due(6005, 24_000_000_000), "automatic write must not reset explicit clock");
    assert!(!s.due(6006, 23_000_000_000), "backwards time cannot repeat a bucket");
    s.explicit(24_100_000_000);
    assert!(!s.due(6007, 43_000_000_000));
    assert!(s.due(6008, 45_000_000_000));
}

#[test]
fn post_only_gate_and_disabled_refresh_preserve_inner_bucket() {
    let mut w = wire();
    w.post_only_until_height = Some(10000);
    w.post_only_until_time = Some("1970-01-01T00:01:00".into());
    let mut s = RefreshState::from_wire(w, 1).unwrap();
    assert!(!s.due(4999, 30_000_000_000));
    assert_eq!(s.inner.last_bucket, 0);
    assert!(s.due(5000, 31_000_000_000), "hardfork + 5000 releases the startup guard");
    let mut w = wire(); w.config.as_mut().unwrap().sgd = None;
    let mut s = RefreshState::from_wire(w, 1).unwrap();
    assert!(!s.due(6000, 100_000_000_000));
    assert_eq!(s.inner.last_bucket, 0);
    let mut w = wire(); w.post_only_until_height = Some(100);
    let mut s = RefreshState::from_wire(w, 1).unwrap();
    assert!(s.due(100, 30_000_000_000), "both post-only thresholds also release startup");
}

#[test]
fn seed_reload_retains_buckets_and_rejects_unknown_or_invalid_state() {
    let mut s = RefreshState::from_wire(wire(), 1).unwrap();
    assert!(s.due(6000, 30_000_000_000));
    let mut w = wire();
    w.outer = Some(s.outer.clone()); w.config.as_mut().unwrap().sog = s.inner.clone();
    let mut reloaded = RefreshState::from_wire(w, 1).unwrap();
    assert!(!reloaded.due(6000, 30_000_000_000));
    assert!(reloaded.due(6001, 33_000_000_000));
    let mut w = wire(); w.outer.as_mut().unwrap().bucket_millis = 0;
    assert!(RefreshState::from_wire(w, 1).is_err());
    let mut w = wire(); w.config.as_mut().unwrap().mou.u[0].push((0, 100));
    assert!(RefreshState::from_wire(w, 1).is_err());
    let mut w = wire(); w.config.as_mut().unwrap().mou.u[1].clear();
    assert!(RefreshState::from_wire(w, 1).is_err());
    let mut w = wire(); w.config = None;
    assert!(RefreshState::from_wire(w, 1).is_err());
    assert!(serde_json::from_value::<WireRefreshConfig>(serde_json::json!({
        "sog":{"last_bucket":0,"bucket_millis":3000},"mou":{"u":[[],[],[]]}
    })).is_err(), "missing enable field must not silently disable refresh");
    let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
    checkpoint["exchange"].as_object_mut().unwrap().remove("msc");
    let accounts = parse_checkpoint_bytes(&rmp_serde::to_vec_named(&checkpoint).unwrap()).unwrap();
    assert!(MarkSeed::from_checkpoint(accounts).is_err(), "missing price history withholds marks, not account parsing");
}

#[test]
fn changed_submission_coverage_withholds_automatic_history_until_reseed() {
    let s = RefreshState::from_wire(wire(), 2).unwrap();
    assert!(s.check_coverage([true, false].into_iter()).is_ok());
    assert!(s.check_coverage([false, false].into_iter()).is_err());
    assert!(s.check_coverage([true, true].into_iter()).is_err());
    assert!(s.check_coverage([true, false, true].into_iter()).is_err());
}
