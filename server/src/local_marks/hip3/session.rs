//! The US stocks' quote sessions in New York time, the US daylight saving rule since 2007 (from 02:00 on March's second
//! Sunday to 02:00 on November's first), as the relayer's external coverage lists them (docs.trade.xyz, US: "Sunday
//! 8:05 PM ET to Friday 8:00 PM ET"):
//! - pre-market 04:00–09:30, regular 09:30–16:00, post-market 16:00–20:00, Monday to Friday, on the SIP (design
//!   000535 r2 revision 2, 000537 r2 revision 1);
//! - overnight 20:05 to the next day's 04:00, begun Sunday to Thursday evening, on BOATS (design 000543): Friday
//!   00:00–04:00 is Thursday's night, Saturday's early hours none, 20:00–20:05 none.
//!
//! A session is one day's (an overnight session the day its evening begins): the offset carry starts over in each.
//! Holidays and early closes are not encoded: with the relayer's external price equal to its oracle and a fresh quote
//! this is an observation gate, not proof that the market is open.

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Weekday};

/// The sessions' bounds, minutes after midnight in New York.
const PRE_OPEN_MINUTE: u32 = 4 * 60;
const REGULAR_OPEN_MINUTE: u32 = 9 * 60 + 30;
const REGULAR_CLOSE_MINUTE: u32 = 16 * 60;
const POST_CLOSE_MINUTE: u32 = 20 * 60;
/// The relayer's overnight coverage starts 20:05, five minutes after the post-market's close.
const OVERNIGHT_OPEN_MINUTE: u32 = 20 * 60 + 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EquitySession {
    Pre,
    Regular,
    Post,
    Overnight,
}

/// One session of one New York day (an overnight session's: the day its evening begins).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EquitySessionId {
    pub(crate) date: NaiveDate,
    pub(crate) session: EquitySession,
}

/// The session unix ms `utc_ms` is in; `None` outside every session.
pub(crate) fn equity_session(utc_ms: u64) -> Option<EquitySessionId> {
    let utc = i64::try_from(utc_ms).ok().and_then(DateTime::from_timestamp_millis)?.naive_utc();
    let local = utc + chrono::Duration::hours(new_york_offset_hours(utc));
    let (date, minute) = (local.date(), minute_of_day(local.time()));
    let session = match minute {
        _ if minute < PRE_OPEN_MINUTE => return overnight_begun(date.pred_opt()?),
        _ if minute < REGULAR_OPEN_MINUTE => EquitySession::Pre,
        _ if minute < REGULAR_CLOSE_MINUTE => EquitySession::Regular,
        _ if minute < POST_CLOSE_MINUTE => EquitySession::Post,
        _ if minute < OVERNIGHT_OPEN_MINUTE => return None,
        _ => return overnight_begun(date),
    };
    (!matches!(date.weekday(), Weekday::Sat | Weekday::Sun)).then_some(EquitySessionId { date, session })
}

/// The overnight session begun on `date`'s evening: Sunday to Thursday.
fn overnight_begun(date: NaiveDate) -> Option<EquitySessionId> {
    (!matches!(date.weekday(), Weekday::Fri | Weekday::Sat)).then_some(EquitySessionId { date, session: EquitySession::Overnight })
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

    fn session(text: &str) -> Option<(NaiveDate, EquitySession)> {
        equity_session(ms(text)).map(|id| (id.date, id.session))
    }

    fn day(month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, month, day).unwrap()
    }

    /// Design 000535 r2 revision 2, 000537 r2 revision 1: a session is one day's; the same session the next day is another.
    #[test]
    fn the_day_sessions_split_at_the_open_and_the_close() {
        // Tuesday 2026-10-27, daylight time: 04:00, 09:30, 16:00, 20:00 EDT are 08:00, 13:30, 20:00, 24:00 UTC
        assert_eq!(session("2026-10-27T08:00:00Z"), Some((day(10, 27), EquitySession::Pre)));
        assert_eq!(session("2026-10-27T13:29:59.999Z"), Some((day(10, 27), EquitySession::Pre)));
        assert_eq!(session("2026-10-27T13:30:00Z"), Some((day(10, 27), EquitySession::Regular)));
        assert_eq!(session("2026-10-27T19:59:59.999Z"), Some((day(10, 27), EquitySession::Regular)));
        assert_eq!(session("2026-10-27T20:00:00Z"), Some((day(10, 27), EquitySession::Post)));
        assert_eq!(session("2026-10-27T23:59:59Z"), Some((day(10, 27), EquitySession::Post)), "New York's Tuesday still");
        assert_eq!(session("2026-10-28T08:00:00Z"), Some((day(10, 28), EquitySession::Pre)), "the next day's pre-market is another session");
        // standard time: 09:30 EST is 14:30 UTC
        assert_eq!(session("2026-11-02T14:29:59Z"), Some((day(11, 2), EquitySession::Pre)));
        assert_eq!(session("2026-11-02T14:30:00Z"), Some((day(11, 2), EquitySession::Regular)));
        // daylight time starts Sunday 2026-03-08 07:00 UTC: Monday 2026-03-09 opens 08:00 UTC
        assert_eq!(session("2026-03-09T08:00:00Z"), Some((day(3, 9), EquitySession::Pre)));
        assert_eq!(session("2026-03-06T08:30:00Z"), Some((day(3, 5), EquitySession::Overnight)), "the Friday before: 03:30 EST is Thursday's night");
        assert_eq!(session("2026-03-06T09:00:00Z"), Some((day(3, 6), EquitySession::Pre)));
    }

    /// Design 000543 §2: the overnight session runs 20:05 to 04:00, begun Sunday to Thursday, named by its evening.
    #[test]
    fn the_overnight_session_belongs_to_its_evening() {
        // Sunday 2026-10-25 20:05 EDT is Monday 00:05 UTC
        assert_eq!(session("2026-10-26T00:04:59Z"), None, "20:00–20:05: no session");
        assert_eq!(session("2026-10-26T00:05:00Z"), Some((day(10, 25), EquitySession::Overnight)));
        // Tuesday 02:00 EDT: Monday's night
        assert_eq!(session("2026-10-27T06:00:00Z"), Some((day(10, 26), EquitySession::Overnight)));
        assert_eq!(session("2026-10-27T07:59:59.999Z"), Some((day(10, 26), EquitySession::Overnight)));
        assert_eq!(session("2026-10-27T08:00:00Z"), Some((day(10, 27), EquitySession::Pre)));
        // across midnight New York time the night is one session
        assert_eq!(session("2026-10-28T03:59:59Z"), session("2026-10-28T04:00:00Z"));
        // Friday 03:59 EDT: Thursday's night; Friday from 20:00 and Saturday's early hours: none
        assert_eq!(session("2026-10-30T07:59:00Z"), Some((day(10, 29), EquitySession::Overnight)));
        assert_eq!(session("2026-10-31T00:05:00Z"), None, "Friday 20:05");
        assert_eq!(session("2026-10-31T05:00:00Z"), None, "Saturday 01:00");
        assert_eq!(session("2026-10-31T15:00:00Z"), None, "Saturday");
        // daylight time ends Sunday 2026-11-01 06:00 UTC: Sunday 20:05 EST is Monday 01:05 UTC
        assert_eq!(session("2026-11-02T01:04:59Z"), None);
        assert_eq!(session("2026-11-02T01:05:00Z"), Some((day(11, 1), EquitySession::Overnight)));
        assert_eq!(session("2026-11-02T08:59:59Z"), Some((day(11, 1), EquitySession::Overnight)), "03:59:59 EST");
        assert_eq!(session("2026-11-02T09:00:00Z"), Some((day(11, 2), EquitySession::Pre)));
    }

    #[test]
    fn the_daylight_saving_sundays() {
        assert_eq!(nth_sunday(2026, 3, 2), NaiveDate::from_ymd_opt(2026, 3, 8).unwrap());
        assert_eq!(nth_sunday(2026, 11, 1), NaiveDate::from_ymd_opt(2026, 11, 1).unwrap());
        assert_eq!(nth_sunday(2027, 3, 2), NaiveDate::from_ymd_opt(2027, 3, 14).unwrap());
        assert_eq!(nth_sunday(2027, 11, 1), NaiveDate::from_ymd_opt(2027, 11, 7).unwrap());
    }
}
