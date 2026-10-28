//! The SIP's candidate hours (design 000535 r2 revision 2): Monday to Friday, 04:00–20:00 in New York, the US daylight
//! saving rule since 2007 (from 02:00 on March's second Sunday to 02:00 on November's first). Holidays and early closes
//! are not encoded: with the relayer's external price equal to its oracle and a fresh SIP quote this is an observation
//! gate, not proof that the market is open.
//!
//! Within them the sessions (design 000537 r2 revision 1): pre-market 04:00–09:30, regular 09:30–16:00, post-market
//! 16:00–20:00, each one day's (the offset carry starts over in each).

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Weekday};

const OPEN_HOUR: u32 = 4;
const CLOSE_HOUR: u32 = 20;
/// The regular session's open and close, minutes after midnight in New York.
const REGULAR_OPEN_MINUTE: u32 = 9 * 60 + 30;
const REGULAR_CLOSE_MINUTE: u32 = 16 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SipSession {
    Pre,
    Regular,
    Post,
}

/// One session of one New York day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SipSessionId {
    pub(crate) date: NaiveDate,
    pub(crate) session: SipSession,
}

/// The session unix ms `utc_ms` is in; `None` outside the candidate hours.
pub(crate) fn sip_session(utc_ms: u64) -> Option<SipSessionId> {
    let utc = i64::try_from(utc_ms).ok().and_then(DateTime::from_timestamp_millis)?.naive_utc();
    let local = utc + chrono::Duration::hours(new_york_offset_hours(utc));
    if matches!(local.weekday(), Weekday::Sat | Weekday::Sun) || !(OPEN_HOUR..CLOSE_HOUR).contains(&local.hour()) {
        return None;
    }
    let minute = minute_of_day(local.time());
    let session = match minute {
        _ if minute < REGULAR_OPEN_MINUTE => SipSession::Pre,
        _ if minute < REGULAR_CLOSE_MINUTE => SipSession::Regular,
        _ => SipSession::Post,
    };
    Some(SipSessionId { date: local.date(), session })
}

/// Whether unix ms `utc_ms` is within the candidate hours.
pub(crate) fn sip_candidate_hours(utc_ms: u64) -> bool {
    sip_session(utc_ms).is_some()
}

fn minute_of_day(time: NaiveTime) -> u32 {
    time.hour() * 60 + time.minute()
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

    /// Design 000537 r2 revision 1: a session is one day's; the same session the next day is another.
    #[test]
    fn the_sessions_split_at_the_open_and_the_close() {
        let tuesday = NaiveDate::from_ymd_opt(2026, 10, 27).unwrap();
        let id = |session| Some(SipSessionId { date: tuesday, session });
        // daylight time: 09:30 EDT is 13:30 UTC, 16:00 EDT 20:00 UTC
        assert_eq!(sip_session(ms("2026-10-27T08:00:00Z")), id(SipSession::Pre));
        assert_eq!(sip_session(ms("2026-10-27T13:29:59.999Z")), id(SipSession::Pre));
        assert_eq!(sip_session(ms("2026-10-27T13:30:00Z")), id(SipSession::Regular));
        assert_eq!(sip_session(ms("2026-10-27T19:59:59.999Z")), id(SipSession::Regular));
        assert_eq!(sip_session(ms("2026-10-27T20:00:00Z")), id(SipSession::Post));
        assert_eq!(sip_session(ms("2026-10-27T23:59:59Z")), id(SipSession::Post), "New York's Tuesday still");
        assert_eq!(sip_session(ms("2026-10-28T00:00:00Z")), None);
        let wednesday = sip_session(ms("2026-10-28T08:00:00Z")).unwrap();
        assert_eq!(wednesday.session, SipSession::Pre);
        assert_ne!(Some(wednesday), id(SipSession::Pre), "the next day's pre-market is another session");
        // standard time: 09:30 EST is 14:30 UTC
        let monday = NaiveDate::from_ymd_opt(2026, 11, 2).unwrap();
        assert_eq!(sip_session(ms("2026-11-02T14:29:59Z")), Some(SipSessionId { date: monday, session: SipSession::Pre }));
        assert_eq!(sip_session(ms("2026-11-02T14:30:00Z")), Some(SipSessionId { date: monday, session: SipSession::Regular }));
        assert_eq!(sip_session(ms("2026-10-31T15:00:00Z")), None, "Saturday");
    }

    #[test]
    fn the_daylight_saving_sundays() {
        assert_eq!(nth_sunday(2026, 3, 2), NaiveDate::from_ymd_opt(2026, 3, 8).unwrap());
        assert_eq!(nth_sunday(2026, 11, 1), NaiveDate::from_ymd_opt(2026, 11, 1).unwrap());
        assert_eq!(nth_sunday(2027, 3, 2), NaiveDate::from_ymd_opt(2027, 3, 14).unwrap());
        assert_eq!(nth_sunday(2027, 11, 1), NaiveDate::from_ymd_opt(2027, 11, 7).unwrap());
    }
}
