//! The SIP's candidate hours (design 000535 r2 revision 2): Monday to Friday, 04:00–20:00 in New York, the US daylight
//! saving rule since 2007 (from 02:00 on March's second Sunday to 02:00 on November's first). Holidays and early closes
//! are not encoded: with the relayer's external price equal to its oracle and a fresh SIP quote this is an observation
//! gate, not proof that the market is open.

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike, Weekday};

const OPEN_HOUR: u32 = 4;
const CLOSE_HOUR: u32 = 20;

/// Whether unix ms `utc_ms` is within the candidate hours.
pub(crate) fn sip_candidate_hours(utc_ms: u64) -> bool {
    let Some(utc) = i64::try_from(utc_ms).ok().and_then(DateTime::from_timestamp_millis).map(|time| time.naive_utc()) else {
        return false;
    };
    let local = utc + chrono::Duration::hours(new_york_offset_hours(utc));
    !matches!(local.weekday(), Weekday::Sat | Weekday::Sun) && (OPEN_HOUR..CLOSE_HOUR).contains(&local.hour())
}

/// UTC to New York: -4 h in daylight saving time, -5 h otherwise.
fn new_york_offset_hours(utc: NaiveDateTime) -> i64 {
    let year = utc.year();
    // 02:00 local: 07:00 UTC in standard time when it starts, 06:00 UTC in daylight time when it ends
    let start = nth_sunday(year, 3, 2).and_hms_opt(7, 0, 0);
    let end = nth_sunday(year, 11, 1).and_hms_opt(6, 0, 0);
    match (start, end) {
        (Some(start), Some(end)) if utc >= start && utc < end => -4,
        _ => -5,
    }
}

/// The `nth` Sunday of `month`.
fn nth_sunday(year: i32, month: u32, nth: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).unwrap_or_else(|| unreachable!("{year}-{month}-01 is a date"));
    let to_sunday = (7 - first.weekday().num_days_from_sunday()) % 7;
    first + chrono::Duration::days(i64::from(to_sunday + 7 * (nth - 1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(text: &str) -> u64 {
        u64::try_from(DateTime::parse_from_rfc3339(text).unwrap().timestamp_millis()).unwrap()
    }

    #[test]
    fn the_candidate_hours_follow_new_york_time() {
        // Tuesday 2026-10-27, daylight time: 04:00–20:00 EDT is 08:00–24:00 UTC
        assert!(!sip_candidate_hours(ms("2026-10-27T07:59:59Z")));
        assert!(sip_candidate_hours(ms("2026-10-27T08:00:00Z")));
        assert!(sip_candidate_hours(ms("2026-10-27T23:59:59Z")));
        assert!(!sip_candidate_hours(ms("2026-10-28T00:00:00Z")));
        // Monday 2026-11-02, standard time (it ended Sunday 2026-11-01 06:00 UTC): 09:00–01:00 UTC
        assert!(!sip_candidate_hours(ms("2026-11-02T08:59:59Z")));
        assert!(sip_candidate_hours(ms("2026-11-02T09:00:00Z")));
        assert!(sip_candidate_hours(ms("2026-11-03T00:59:59Z")));
        assert!(!sip_candidate_hours(ms("2026-11-03T01:00:00Z")));
        // the weekend: Saturday 2026-10-31 in New York, and Friday's evening after 20:00
        assert!(!sip_candidate_hours(ms("2026-10-31T15:00:00Z")));
        assert!(!sip_candidate_hours(ms("2026-10-31T00:30:00Z")));
        assert!(sip_candidate_hours(ms("2026-10-30T23:30:00Z")));
        // daylight time starts Sunday 2026-03-08 07:00 UTC: Monday 2026-03-09 opens 08:00 UTC
        assert!(sip_candidate_hours(ms("2026-03-09T08:00:00Z")));
        assert!(!sip_candidate_hours(ms("2026-03-06T08:30:00Z")), "the Friday before: 03:30 EST");
    }

    #[test]
    fn the_daylight_saving_sundays() {
        assert_eq!(nth_sunday(2026, 3, 2), NaiveDate::from_ymd_opt(2026, 3, 8).unwrap());
        assert_eq!(nth_sunday(2026, 11, 1), NaiveDate::from_ymd_opt(2026, 11, 1).unwrap());
        assert_eq!(nth_sunday(2027, 3, 2), NaiveDate::from_ymd_opt(2027, 3, 14).unwrap());
        assert_eq!(nth_sunday(2027, 11, 1), NaiveDate::from_ymd_opt(2027, 11, 7).unwrap());
    }
}
