use super::controller;
use axum::Router;

pub fn get_router() -> Router {
    Router::new().route("/", axum::routing::get(controller::root))
}
