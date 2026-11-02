//! Who acts for a pending order (batch 2g, hyperliquid-trade design 000621 r3): the vault its signed action names, else
//! its signer, recovered from its signature. HL signs an L1 action as its Python SDK's `sign_l1_action`:
//! keccak(msgpack(action) + nonce (8 bytes, big-endian) + 0x00, or 0x01 and the vault address, + 0x00 and
//! `expiresAfter` (8 bytes) when given) is the `connectionId` of an EIP-712 `Agent {source: "a", connectionId}` in the
//! domain {name "Exchange", version "1", chainId 1337, verifyingContract 0x0}. The signer is the account itself or an API
//! wallet it approved (research `mempool_pending_book_20261031`, bm 10-31 20:15, one minute: 24,951 order actions
//! without a vault, 1,196 signers, each acting for one account by the block responses).

use std::sync::LazyLock;

use alloy::primitives::{Address, B256, U256, keccak256};
use alloy_primitives::Signature;
use serde::Deserialize;
use sonic_rs::{JsonContainerTrait, JsonType, JsonValueTrait, LazyValue, PointerNode, PointerTree, Value};

use crate::accounts::replica::SignedAction;

/// Who acts for an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ActingIdentity {
    /// The vault or subaccount its action names.
    Vault(Address),
    /// Its signer, acting for one account (bm 10-31: every signer seen did): an account signing through several API
    /// wallets is as many identities.
    Signer(Address),
}

impl ActingIdentity {
    /// The vault a signed action names, else its `signer` when known.
    pub(crate) fn of(vault: Option<Address>, signer: Option<Address>) -> Option<Self> {
        vault.map(Self::Vault).or_else(|| signer.map(Self::Signer))
    }
}

/// A signature as HL writes one: `v` 27/28, or the recovery id 0/1 itself (bm 10-31: both in one block).
#[derive(Deserialize)]
struct WireSignature {
    r: U256,
    s: U256,
    v: u64,
}

/// keccak of the EIP-712 domain {name "Exchange", version "1", chainId 1337, verifyingContract 0x0}.
static DOMAIN_SEPARATOR: LazyLock<B256> = LazyLock::new(|| {
    let mut domain = keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)").to_vec();
    domain.extend_from_slice(keccak256("Exchange").as_slice());
    domain.extend_from_slice(keccak256("1").as_slice());
    domain.extend_from_slice(&U256::from(1337).to_be_bytes::<32>());
    domain.extend_from_slice(&[0; 32]);
    keccak256(domain)
});

/// keccak of `Agent(string source,bytes32 connectionId)` and of mainnet's source, "a".
static AGENT_TYPE: LazyLock<B256> = LazyLock::new(|| keccak256("Agent(string source,bytes32 connectionId)"));
static MAINNET_SOURCE: LazyLock<B256> = LazyLock::new(|| keccak256("a"));

/// The signer of `signed`, whose action reads `action` (its JSON as written): `None` when it cannot be told - an action
/// holding a number msgpack would not write as an integer, as HL hashes it (a float, one past 64 bits), or a signature
/// no key signed. An `Err` when its nonce, signature, vault or expiry does not read.
pub(crate) fn signer(signed: &SignedAction<'_>, action: &str) -> Result<Option<Address>, String> {
    let nonce: u64 = required(signed.nonce.as_ref(), "nonce")?;
    let signature: WireSignature = required(signed.signature.as_ref(), "signature")?;
    let action: Value = sonic_rs::from_str(action).map_err(|err| format!("action: {err}"))?;
    let mut hashed = Vec::new();
    if !pack(&action, &mut hashed)? {
        return Ok(None);
    }
    hashed.extend_from_slice(&nonce.to_be_bytes());
    match signed.vault()? {
        Some(vault) => {
            hashed.push(1);
            hashed.extend_from_slice(vault.as_slice());
        }
        None => hashed.push(0),
    }
    if let Some(expires_after_ms) = signed.expires_after_ms()? {
        hashed.push(0);
        hashed.extend_from_slice(&expires_after_ms.to_be_bytes());
    }
    let Some(y_parity) = y_parity(signature.v) else { return Ok(None) };
    let digest = agent_digest(keccak256(hashed));
    Ok(Signature::new(signature.r, signature.s, y_parity).recover_address_from_prehash(&digest).ok())
}

/// The actions at `paths` of `line`, as written, in the paths' order: one pass over the line for them all (review
/// 000621 r2: 64 bm blocks, 201 actions, p95 0.22 ms against 1.50 ms read one by one).
pub(crate) fn actions_written(line: &str, paths: impl IntoIterator<Item = Vec<PointerNode>>) -> Result<Vec<LazyValue<'_>>, String> {
    let mut tree = PointerTree::new();
    for path in paths {
        tree.add_path(path);
    }
    sonic_rs::get_many(line, &tree).map_err(|err| format!("signed actions as written: {err}"))
}

/// A signed action's envelope field its signer needs.
fn required<T: for<'de> Deserialize<'de>>(raw: Option<&LazyValue<'_>>, name: &str) -> Result<T, String> {
    let raw = raw.ok_or_else(|| format!("without {name}"))?;
    sonic_rs::from_str(raw.as_raw_str()).map_err(|err| format!("{name} {}: {err}", raw.as_raw_str()))
}

/// The recovery id `v` stands for; `None` for any other.
const fn y_parity(v: u64) -> Option<bool> {
    match v {
        0 | 27 => Some(false),
        1 | 28 => Some(true),
        _ => None,
    }
}

/// The EIP-712 digest of `Agent {source: "a", connectionId}`.
fn agent_digest(connection_id: B256) -> B256 {
    let mut agent = AGENT_TYPE.to_vec();
    agent.extend_from_slice(MAINNET_SOURCE.as_slice());
    agent.extend_from_slice(connection_id.as_slice());
    let mut digest = vec![0x19, 0x01];
    digest.extend_from_slice(DOMAIN_SEPARATOR.as_slice());
    digest.extend_from_slice(keccak256(agent).as_slice());
    keccak256(digest)
}

/// `value` as msgpack onto `out`, as the SDK's `msgpack.packb` of the parsed JSON writes it: an object's keys in their
/// order, each integer and string in its shortest form. `false` when a number is not a 64-bit integer.
fn pack(value: &Value, out: &mut Vec<u8>) -> Result<bool, String> {
    let written = match value.get_type() {
        JsonType::Null => rmp::encode::write_nil(out).map_err(|err| err.to_string()),
        JsonType::Boolean => rmp::encode::write_bool(out, value.as_bool().ok_or("a boolean that is not one")?).map_err(|err| err.to_string()),
        JsonType::Number => {
            if let Some(unsigned) = value.as_u64() {
                rmp::encode::write_uint(out, unsigned).map(drop).map_err(|err| err.to_string())
            } else if let Some(signed) = value.as_i64() {
                rmp::encode::write_sint(out, signed).map(drop).map_err(|err| err.to_string())
            } else {
                return Ok(false);
            }
        }
        JsonType::String => pack_str(value.as_str().ok_or("a string that is not one")?, out),
        JsonType::Array => {
            let array = value.as_array().ok_or("an array that is not one")?;
            rmp::encode::write_array_len(out, length(array.len())?).map_err(|err| err.to_string())?;
            for item in array {
                if !pack(item, out)? {
                    return Ok(false);
                }
            }
            Ok(())
        }
        JsonType::Object => {
            let object = value.as_object().ok_or("an object that is not one")?;
            rmp::encode::write_map_len(out, length(object.len())?).map_err(|err| err.to_string())?;
            for (key, item) in object {
                pack_str(key, out)?;
                if !pack(item, out)? {
                    return Ok(false);
                }
            }
            Ok(())
        }
    };
    written.map(|()| true)
}

fn pack_str(text: &str, out: &mut Vec<u8>) -> Result<(), String> {
    rmp::encode::write_str(out, text).map_err(|err| err.to_string())
}

fn length(len: usize) -> Result<u32, String> {
    u32::try_from(len).map_err(|_| format!("{len} items, past msgpack's 32-bit lengths"))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::hex;

    use super::*;
    use crate::pending::{
        blocks::{parse_block, tests::block_of},
        mempool::parse_line,
        refusals::{Refusal, TakerKey, TakerOutcome},
    };

    /// bm 10-31 block 1204360008's signed actions, the signer of each recovered by the research's Python
    /// (`signer_recovery.py`, the SDK's `sign_l1_action` reversed); the first two the same in the mempool.
    /// An API wallet's reduce-only IOC for account 0xc2502a3c1b3d0c0b98d6f8f29b130610bc8dbbee.
    const AGENT: &str = r#"{"signature":{"r":"0xff1afde8182ffcb4c2f2fc78d52ed3cbf421accceb9b9ba2fca1ecf9d2e63992","s":"0x1e1cac0b64c6629d2898e98143c4e11962a9262f5975848cf92aa25c3431945b","v":28},"action":{"type":"order","orders":[{"a":224,"b":false,"p":"567.88","s":"0.109","r":true,"t":{"limit":{"tif":"Ioc"}}}],"grouping":"na"},"nonce":1793477702091}"#;
    const AGENT_SIGNER: &str = "0xb7ebaba907e793b1cb384cc15babc446bcfc7708";
    const AGENT_TX: &str = r#"["2026-10-31T20:14:59.963633829",{"tx_hash":"0x696548db4981a3ada6205d0f93a6c2ae9fec861ebd0fae575bf14fcbf0ff0382","signed_actions":[ACTION]}]"#;
    /// An account's own GTC, with a builder.
    const OWN: &str = r#"{"signature":{"r":"0xbbbae2bd8325507d9d7c380592ad30066b427b393cba7883a2a0d4d26c7ca7d1","s":"0x297fe1ee16ed3fc6a12f79e32be22af59a196d0dc802c54c9de569e71d0e4dad","v":27},"action":{"type":"order","orders":[{"a":223,"b":false,"p":"3.0993","s":"79","r":false,"t":{"limit":{"tif":"Gtc"}},"c":"0xa3e08e57eaee734b44fb701e11f1bcb2"}],"grouping":"na","builder":{"b":"0xa9ab442f9dfe752dc74b666c41e7a0498baf8687","f":20}},"nonce":1793477699774}"#;
    const OWN_SIGNER: &str = "0x9212b44b3578dc67a9774d5629a3dab805a9f8fb";
    const OWN_TX: &str = r#"["2026-10-31T20:14:59.984422339",{"tx_hash":"0xfd677fc1238795f06890fd9a647d979506581bc5ff04a09ac91d080fa9d8a7b4","signed_actions":[ACTION]}]"#;
    /// A vault's IOC with an `expiresAfter`: its hash covers both.
    const VAULT_EXPIRING: &str = r#"{"signature":{"r":"0x2f95947f0ec9c1a82864a1b3d028eade65b3fdf7f8f1f39476ad4c82bdd69475","s":"0x5a6c6ecc7ed000514f0ccb3eed8833f82afce4abb38441d31a75c4142d96505f","v":27},"vaultAddress":"0xbff945932b5ff40699e9f12fa40927c5667df619","action":{"type":"order","orders":[{"a":64,"b":true,"p":"16.203","s":"30858.48","r":false,"t":{"limit":{"tif":"Ioc"}},"c":"0x23346d2b37cf227f247465cb9ee81fff"}],"grouping":{"p":1}},"nonce":1793477699506,"expiresAfter":1793477709506}"#;
    const VAULT_EXPIRING_SIGNER: &str = "0xf6b4e1c9f8b7ab838d965b56bc02aff7605fb465";
    /// A GTC signed with `v` 1, the recovery id itself.
    const V_AS_ID: &str = r#"{"signature":{"r":"0x4c0aa8831ccd58487b85a48bcca0ba1b85ed70e8fac0e334975c9f1928bb42b1","s":"0x7bb0508e12ec202d0208c7f6449ca5b26f53b1687bf777ad9708bbb781a183ce","v":1},"action":{"type":"order","orders":[{"a":223,"b":true,"p":"2.8265","s":"20","r":false,"t":{"limit":{"tif":"Gtc"}},"c":"0x1371bf93dc9c5be9b483718808e63a70"}],"grouping":"na","builder":{"b":"0x9b451f8941240db8bedc99bff8917a2ed9550074","f":10}},"nonce":1793477699533}"#;
    const V_AS_ID_SIGNER: &str = "0xb6ade49333a6526646ad098142434ed66290e15f";

    fn address(text: &str) -> Address {
        text.parse().unwrap()
    }

    /// The signer of a signed action written `signed`.
    fn signer_of(signed: &str) -> Option<Address> {
        let wire: SignedAction<'_> = sonic_rs::from_str(signed).unwrap();
        signer(&wire, sonic_rs::get(signed, ["action"]).unwrap().as_raw_str()).unwrap()
    }

    fn packed(json: &str) -> Option<String> {
        let mut out = Vec::new();
        pack(&sonic_rs::from_str(json).unwrap(), &mut out).unwrap().then(|| hex::encode(out))
    }

    /// Python `msgpack.packb(json.loads(...))` (msgpack 1.1.1), byte for byte: integers' and lengths' shortest forms
    /// at each boundary, keys in their order, escapes read.
    #[test]
    fn an_action_packs_as_the_sdk_packs_it() {
        let goldens = [
            (
                r#"{"type":"order","orders":[{"a":224,"b":false,"p":"567.88","s":"0.109","r":true,"t":{"limit":{"tif":"Ioc"}}}],"grouping":"na"}"#,
                "83a474797065a56f72646572a66f72646572739186a161cce0a162c2a170a63536372e3838a173a5302e313039a172c3a17481a56c696d697481a3746966a3496f63a867726f7570696e67a26e61",
            ),
            ("[0,127,128,255,256,65535,65536,4294967295,4294967296,18446744073709551615]", "9a007fcc80ccffcd0100cdffffce00010000ceffffffffcf0000000100000000cfffffffffffffffff"),
            (
                "[-1,-32,-33,-128,-129,-32768,-32769,-2147483648,-2147483649,-9223372036854775808]",
                "9affe0d0dfd080d1ff7fd18000d2ffff7fffd280000000d3ffffffff7fffffffd38000000000000000",
            ),
            (r#"{"z":null,"a":true,"m":false,"e":{},"l":[]}"#, "85a17ac0a161c3a16dc2a16580a16c90"),
            (
                r#"["é\n\"q\"",[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15],[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]]"#,
                "93a6c3a90a2271229f0102030405060708090a0b0c0d0e0fdc00100102030405060708090a0b0c0d0e0f10",
            ),
            (
                r#"{"k0":0,"k1":1,"k2":2,"k3":3,"k4":4,"k5":5,"k6":6,"k7":7,"k8":8,"k9":9,"k10":10,"k11":11,"k12":12,"k13":13,"k14":14,"k15":15}"#,
                "de0010a26b3000a26b3101a26b3202a26b3303a26b3404a26b3505a26b3606a26b3707a26b3808a26b3909a36b31300aa36b31310ba36b31320ca36b31330da36b31340ea36b31350f",
            ),
            (
                r#"{"b":1,"a":{"d":[{"y":2,"x":1}],"c":"0x9b451f8941240db8bedc99bff8917a2ed9550074"}}"#,
                "82a16201a16182a1649182a17902a17801a163d92a307839623435316638393431323430646238626564633939626666383931376132656439353530303734",
            ),
        ];
        for (json, golden) in goldens {
            assert_eq!(packed(json).as_deref(), Some(golden), "{json}");
        }
        // strings at each length boundary: fixstr to 31, str8 to 255, then str16
        for (len, header) in [(0, "a0"), (31, "bf"), (32, "d920"), (255, "d9ff"), (256, "da0100")] {
            assert_eq!(packed(&format!(r#""{}""#, "x".repeat(len))), Some(format!("{header}{}", "78".repeat(len))), "{len}");
        }
    }

    /// A float, or an integer past 64 bits: not as HL hashes it, the signer unknown.
    #[test]
    fn an_action_with_a_number_not_a_64_bit_integer_does_not_pack() {
        assert_eq!(packed(r#"{"a":[1,2.5]}"#), None);
        assert_eq!(packed("18446744073709551616"), None);
        assert_eq!(signer_of(&AGENT.replace(r#""a":224"#, r#""a":224.0"#)), None);
    }

    #[test]
    fn a_signer_is_recovered_as_the_sdk_signs() {
        for (signed, expected) in [(AGENT, AGENT_SIGNER), (OWN, OWN_SIGNER), (VAULT_EXPIRING, VAULT_EXPIRING_SIGNER), (V_AS_ID, V_AS_ID_SIGNER)] {
            assert_eq!(signer_of(signed), Some(address(expected)), "{signed}");
        }
        // what the hash covers: the action, the vault, the expiry
        assert_ne!(signer_of(&AGENT.replace("567.88", "567.89")), Some(address(AGENT_SIGNER)));
        assert_ne!(signer_of(&VAULT_EXPIRING.replace("1793477709506", "1793477709507")), Some(address(VAULT_EXPIRING_SIGNER)));
        let other_vault = VAULT_EXPIRING.replace("0xbff945932b5ff40699e9f12fa40927c5667df619", "0xbff945932b5ff40699e9f12fa40927c5667df618");
        assert_ne!(signer_of(&other_vault), Some(address(VAULT_EXPIRING_SIGNER)));
        // a `v` that names no recovery id
        assert_eq!(signer_of(&AGENT.replace(r#""v":28"#, r#""v":29"#)), None);
    }

    /// A signed action needing its signer without a nonce or a signature does not read.
    #[test]
    fn a_signed_action_without_its_nonce_or_signature_does_not_read() {
        for unsigned in [AGENT.replace(r#","nonce":1793477702091"#, ""), AGENT.replace(r#""signature":{"r":"0xff1afde8182ffcb4c2f2fc78d52ed3cbf421accceb9b9ba2fca1ecf9d2e63992","s":"0x1e1cac0b64c6629d2898e98143c4e11962a9262f5975848cf92aa25c3431945b","v":28},"#, "")] {
            let wire: SignedAction<'_> = sonic_rs::from_str(&unsigned).unwrap();
            assert!(signer(&wire, sonic_rs::get(&unsigned, ["action"]).unwrap().as_raw_str()).is_err(), "{unsigned}");
            assert!(parse_line(&AGENT_TX.replace("ACTION", &unsigned)).is_err());
        }
    }

    /// The same signed action in the mempool and in its block: one identity by both readings (design 000621 r3). A vault's
    /// is the vault, unrecovered.
    #[test]
    fn the_mempool_and_the_block_log_tell_the_same_identity() {
        let margin = |user: &str, asset: u32| {
            format!(r#"{{"user":"{user}","res":{{"status":"ok","response":{{"type":"order","data":{{"statuses":[{{"error":"Insufficient margin to place order. asset={asset}"}}]}}}}}}}}"#)
        };
        let cases = [
            // each block's order as written: the agent's reduce-only (`"r":true`), the owner's not
            (AGENT, AGENT_TX, AGENT_SIGNER, "0xc2502a3c1b3d0c0b98d6f8f29b130610bc8dbbee", 224, false, true),
            (OWN, OWN_TX, OWN_SIGNER, OWN_SIGNER, 223, false, false),
        ];
        for (signed, tx, signer, user, asset, is_buy, reduce_only) in cases {
            let mempool = parse_line(&tx.replace("ACTION", signed)).unwrap();
            assert_eq!(mempool.actions[0].signer, Some(address(signer)));
            let block = parse_block(&block_of(&[signed], &[&margin(user, asset)])).unwrap();
            let key = TakerKey { identity: ActingIdentity::Signer(address(signer)), asset, is_buy };
            assert_eq!(block.taker_outcomes, [TakerOutcome { key, reduce_only, refused: Some(Refusal::Margin) }]);
        }
        let vault = address("0xbff945932b5ff40699e9f12fa40927c5667df619");
        let block = parse_block(&block_of(&[VAULT_EXPIRING], &[&margin("0xbff945932b5ff40699e9f12fa40927c5667df619", 64)])).unwrap();
        assert_eq!(block.taker_outcomes, [TakerOutcome { key: TakerKey { identity: ActingIdentity::Vault(vault), asset: 64, is_buy: true }, reduce_only: false, refused: Some(Refusal::Margin) }]);
    }
}
