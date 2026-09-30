use arc_swap::ArcSwap;
use rand::RngExt;
use std::ops::{Add, Deref};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

use crate::api::plugin::{AuthContext, AuthPlugin, LoginIdentityContext};

use super::RequestResource;

pub const USERNAME: &str = "username";

pub const PASSWORD: &str = "password";

pub(crate) const ACCESS_TOKEN: &str = "accessToken";

#[allow(dead_code)]
pub(crate) const TOKEN_TTL: &str = "tokenTtl";

/// Http login AuthPlugin.
pub struct HttpLoginAuthPlugin {
    login_identity: ArcSwap<LoginIdentityContext>,
    next_login_refresh: ArcSwap<Instant>,
}

impl Default for HttpLoginAuthPlugin {
    fn default() -> Self {
        Self {
            login_identity: ArcSwap::from_pointee(LoginIdentityContext::default()),
            next_login_refresh: ArcSwap::from_pointee(Instant::now()),
        }
    }
}

#[async_trait::async_trait]
impl AuthPlugin for HttpLoginAuthPlugin {
    async fn login(&self, server_list: Arc<Vec<String>>, auth_context: Arc<AuthContext>) {
        let now_instant = Instant::now();
        if now_instant.le(self.next_login_refresh.load().deref()) {
            tracing::debug!("Http login return because now_instant lte next_login_refresh.");
            return;
        }

        let username = auth_context
            .params
            .get(USERNAME)
            .expect("Username parameter should exist for HTTP auth")
            .to_owned();
        let password = auth_context
            .params
            .get(PASSWORD)
            .expect("Password parameter should exist for HTTP auth")
            .to_owned();

        let server_addr = {
            // random one
            server_list
                .get(rand::rng().random_range(0..server_list.len()))
                .expect("Server list should not be empty")
                .to_string()
        };

        let scheme = if cfg!(feature = "tls") {
            "https"
        } else {
            "http"
        };
        let login_url = format!("{scheme}://{server_addr}/nacos/v1/auth/login");

        let login_response = request_login(&login_url, &username, &password).await;

        if let Some(login_response) = login_response {
            let delay_sec = login_response.token_ttl / 10;
            let new_login_identity = Arc::new(
                LoginIdentityContext::default()
                    .add_context(ACCESS_TOKEN, login_response.access_token),
            );
            self.login_identity.store(new_login_identity);

            self.next_login_refresh
                .store(Arc::new(Instant::now().add(Duration::from_secs(delay_sec))));
        }
    }

    fn get_login_identity(&self, _: RequestResource) -> LoginIdentityContext {
        self.login_identity.load().deref().deref().to_owned()
    }
}

#[tracing::instrument(skip_all, fields(method = "POST", endpoint = "/nacos/v1/auth/login"))]
async fn request_login(
    login_url: &str,
    username: &str,
    password: &str,
) -> Option<HttpLoginResponse> {
    let response = match crate::common::remote::http_client()
        .post(login_url)
        .form(&[(USERNAME, username), (PASSWORD, password)])
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
    {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(status = ?error.status(), error = %error.without_url(), "HTTP login request failed");
            return None;
        }
    };
    let status = response.status();
    let body = match response.text().await {
        Ok(body) => body,
        Err(error) => {
            tracing::error!(%status, error = %error.without_url(), "HTTP login response read failed");
            return None;
        }
    };
    match serde_json::from_str(&body) {
        Ok(response) => {
            tracing::debug!(%status, "HTTP login succeeded");
            Some(response)
        }
        Err(error) => {
            tracing::error!(%status, category = ?error.classify(), line = error.line(), column = error.column(), "HTTP login response is invalid");
            None
        }
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpLoginResponse {
    access_token: String,
    token_ttl: u64,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::api::plugin::{AuthContext, AuthPlugin, HttpLoginAuthPlugin, RequestResource};

    #[tokio::test]
    async fn login_uses_form_body_and_redacts_response_logs() {
        use axum::{
            Router,
            http::{HeaderMap, StatusCode, Uri},
            routing::post,
        };
        use tracing::instrument::WithSubscriber;

        for (status, body, succeeds) in [
            (
                StatusCode::OK,
                r#"{"accessToken":"token-sentinel","tokenTtl":18000}"#,
                true,
            ),
            (
                StatusCode::FORBIDDEN,
                r#"{"accessToken":"token-sentinel","tokenTtl":18000}"#,
                false,
            ),
            (
                StatusCode::OK,
                r#"{"accessToken":"token-sentinel","tokenTtl":"password-sentinel"}"#,
                false,
            ),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind local HTTP login test server");
            let address = listener
                .local_addr()
                .expect("read local HTTP login test address");
            let app = Router::new().route(
                "/nacos/v1/auth/login",
                post(
                    move |uri: Uri, headers: HeaderMap, form: String| async move {
                        assert!(uri.query().is_none());
                        assert_eq!(headers["content-type"], "application/x-www-form-urlencoded");
                        let values = url::form_urlencoded::parse(form.as_bytes())
                            .collect::<std::collections::HashMap<_, _>>();
                        assert_eq!(values["username"], "username-sentinel +");
                        assert_eq!(values["password"], "password-sentinel &=");
                        (status, body)
                    },
                ),
            );
            let server = tokio::spawn(async move {
                axum::serve(listener, app)
                    .await
                    .expect("serve local HTTP login test requests")
            });
            let log_path =
                std::env::temp_dir().join(format!("nacos-auth-log-{}", rand::random::<u64>()));
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(Arc::new(
                    std::fs::File::create(&log_path).expect("create HTTP auth test log"),
                ))
                .finish();
            let response = super::request_login(
                &format!("http://{address}/nacos/v1/auth/login"),
                "username-sentinel +",
                "password-sentinel &=",
            )
            .with_subscriber(subscriber)
            .await;
            assert_eq!(response.is_some(), succeeds);
            let logs = std::fs::read_to_string(&log_path).expect("read HTTP auth test log");
            for secret in ["username-sentinel", "password-sentinel", "token-sentinel"] {
                assert!(
                    !logs.contains(secret),
                    "HTTP auth logs must not contain credentials or response values"
                );
            }
            assert!(logs.contains("/nacos/v1/auth/login"));
            std::fs::remove_file(log_path).expect("remove HTTP auth test log");
            server.abort();
            assert!(
                server
                    .await
                    .expect_err("aborted HTTP login test server must stop")
                    .is_cancelled()
            );
        }
    }

    #[tokio::test]
    #[ignore]
    #[cfg(not(tarpaulin))]
    async fn test_http_login_auth_plugin() {
        crate::test_config::setup_log();

        let http_auth_plugin = HttpLoginAuthPlugin::default();
        let server_list = Arc::new(vec!["127.0.0.1:8848".to_string()]);

        let auth_context = Arc::new(
            AuthContext::default()
                .add_param(crate::api::plugin::USERNAME, "nacos")
                .add_param(crate::api::plugin::PASSWORD, "nacos"),
        );

        http_auth_plugin
            .login(server_list.clone(), auth_context.clone())
            .await;
        let login_identity_1 = http_auth_plugin.get_login_identity(RequestResource::default());
        assert_eq!(login_identity_1.contexts.len(), 1);

        tokio::time::sleep(tokio::time::Duration::from_millis(111)).await;

        http_auth_plugin.login(server_list, auth_context).await;
        let login_identity_2 = http_auth_plugin.get_login_identity(RequestResource::default());
        assert_eq!(login_identity_1.contexts, login_identity_2.contexts)
    }
}
