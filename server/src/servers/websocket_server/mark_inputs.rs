//! `GET /extMarkInputs?coin=`: the local marks' venue feeds and each coin's listings (design 000454 batch 1).

use std::sync::Arc;

use serde::Deserialize;
use serde_json::json;

use super::coin_positions::json_response;
use crate::{
    local_marks::now_ms,
    local_marks::{feeds::LocalMarkFeeds, view::inputs_view},
};

#[derive(Deserialize)]
pub(super) struct MarkInputsQuery {
    coin: Option<String>,
}

#[expect(clippy::unused_async, reason = "an axum handler")]
pub(super) async fn mark_inputs_handler(
    axum::extract::Query(query): axum::extract::Query<MarkInputsQuery>,
    feeds: Arc<LocalMarkFeeds>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    match inputs_view(&feeds, query.coin.as_deref(), now_ms()) {
        Ok(view) => {
            #[expect(clippy::expect_used, reason = "string keys and integers only; serializing cannot fail")]
            let body = serde_json::to_vec(&view).expect("the inputs view serializes");
            json_response(StatusCode::OK, None, body.into())
        }
        Err(err) => json_response(StatusCode::NOT_FOUND, None, json!({"error": err}).to_string().into()),
    }
}
