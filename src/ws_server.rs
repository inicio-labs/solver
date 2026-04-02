use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use solver::events::SolverEvent;
use tokio::sync::broadcast;

#[derive(Clone)]
struct AppState {
    event_tx: broadcast::Sender<SolverEvent>,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    println!("[ws] new client connecting");
    ws.on_upgrade(move |socket| handle_socket(socket, state.event_tx))
}

async fn handle_socket(mut socket: WebSocket, event_tx: broadcast::Sender<SolverEvent>) {
    println!("[ws] client connected, subscribing to events");
    let mut rx = event_tx.subscribe();

    loop {
        match rx.recv().await {
            Ok(event) => {
                let json = match serde_json::to_string(&event) {
                    Ok(j) => j,
                    Err(e) => {
                        eprintln!("[ws] serialize error: {}", e);
                        continue;
                    }
                };
                println!("[ws] sending {} bytes", json.len());
                if socket.send(Message::Text(json.into())).await.is_err() {
                    println!("[ws] client disconnected");
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                eprintln!("[ws] client lagged, skipped {} events", n);
            }
            Err(broadcast::error::RecvError::Closed) => {
                println!("[ws] broadcast channel closed");
                break;
            }
        }
    }
}

pub fn build_router(event_tx: broadcast::Sender<SolverEvent>) -> Router {
    let state = AppState { event_tx };
    Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state)
}
