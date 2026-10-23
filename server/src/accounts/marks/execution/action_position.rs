//! Optional execution locator carried by node L1 fill hashes. BM 2026-10-23: 452 ordinary
//! taker fills matched receipt oids using the execution-sorted index, not proposal position.
//! Official txDetails independently resolves DRAM1193762464 and SNDK1193771663 to io setOracle.
//! This observed wire encoding is not an EVM hash decoder or an authentication check. Unknown,
//! zero, noncanonical and nested locators supply no timing evidence; they never stop replay.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct L1ActionPosition {
    pub(crate) height: u64,
    pub(crate) index: u64,
}

impl L1ActionPosition {
    pub(crate) fn of(hash: &str) -> Option<Self> {
        let bytes: alloy::primitives::B256 = hash.parse().ok()?;
        let mut tail = &bytes.as_slice()[10..];
        let height = integer(&mut tail)?;
        let index = integer(&mut tail)?;
        // Observed ordinary outer actions have a zero third locator. Nested/system variants
        // are intentionally not promoted to the outer action's timing.
        if height == 0 || integer(&mut tail)? != 0 { return None; }
        Some(Self { height, index })
    }
}

fn integer(bytes: &mut &[u8]) -> Option<u64> {
    let (&len, rest) = bytes.split_first()?;
    let len = usize::from(len);
    if len > 8 { return None; }
    let (value, rest) = rest.split_at_checked(len)?;
    if value.first() == Some(&0) { return None; }
    let mut encoded = [0; 8];
    encoded[8 - len..].copy_from_slice(value);
    *bytes = rest;
    Some(u64::from_be_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn real_oracle_hashes_and_unknown_locators() {
        for (hash, height, index) in [
            ("0x5826fe519a900bc159a00447275ea0021349003735932a93fbefa9a45993e5ab", 1_193_762_464, 4_937),
            ("0x113b5a1a5b6f723612b5044727828f0203f300fff6629108b504056d1a634c20", 1_193_771_663, 1_011),
        ] {
            assert_eq!(L1ActionPosition::of(hash), Some(L1ActionPosition { height, index }));
            let mut bytes = hash.parse::<alloy::primitives::B256>().unwrap();
            bytes[10] = 9;
            assert_eq!(L1ActionPosition::of(&bytes.to_string()), None);
            bytes[10] = 4; bytes[11] = 0;
            assert_eq!(L1ActionPosition::of(&bytes.to_string()), None);
        }
        for hash in ["bad", "0x00", "0x0000000000000000000000000000000000000000000000000000000000000000",
            "0xd65a3ebd325bc057d7d3044723ecf60202ea0101cd5edf297a22ea0ff15f9a42"] {
            assert_eq!(L1ActionPosition::of(hash), None);
        }
    }
}
