#[cfg(feature = "auth-by-http")]
mod auth_by_http;
#[cfg(feature = "auth-by-http")]
pub use auth_by_http::*;

#[cfg(feature = "auth-by-aliyun")]
mod auth_by_aliyun_ram;
#[cfg(feature = "auth-by-aliyun")]
pub use auth_by_aliyun_ram::*;

use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{Instrument, debug, debug_span, info};

use crate::common::remote::grpc::task_group::TaskGroup;
use crate::common::remote::server_list::ServerListProvider;

/// Auth plugin in Client.
/// This api may change in the future, please forgive me if you customize the implementation.
#[async_trait::async_trait]
pub trait AuthPlugin: Send + Sync {
    /// Login with [`AuthContext`], Note that this method will be scheduled continuously.
    async fn login(&self, server_list: Arc<Vec<String>>, auth_context: Arc<AuthContext>);

    /// Get the [`LoginIdentityContext`].
    fn get_login_identity(&self, resource: RequestResource) -> LoginIdentityContext;
}

#[derive(Clone, Default)]
pub struct AuthContext {
    pub(crate) params: HashMap<String, String>,
}

impl AuthContext {
    /// Add the param.
    pub fn add_param(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.params.insert(key.into(), val.into());
        self
    }

    /// Add the params.
    pub fn add_params(mut self, map: HashMap<String, String>) -> Self {
        self.params.extend(map);
        self
    }
}

#[derive(Clone, Default)]
pub struct LoginIdentityContext {
    pub(crate) contexts: HashMap<String, String>,
}

impl LoginIdentityContext {
    /// Add the context.
    pub fn add_context(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.contexts.insert(key.into(), val.into());
        self
    }

    /// Add the contexts.
    pub fn add_contexts(mut self, map: HashMap<String, String>) -> Self {
        self.contexts.extend(map);
        self
    }
}

/// Noop AuthPlugin.
#[derive(Default)]
pub(crate) struct NoopAuthPlugin {
    login_identity: LoginIdentityContext,
}

#[async_trait::async_trait]
impl AuthPlugin for NoopAuthPlugin {
    #[allow(unused_variables)]
    async fn login(&self, server_list: Arc<Vec<String>>, auth_context: Arc<AuthContext>) {
        // noop
    }

    fn get_login_identity(&self, _: RequestResource) -> LoginIdentityContext {
        // noop
        self.login_identity.clone()
    }
}

pub(crate) async fn init_auth_plugin(
    auth_plugin: Arc<dyn AuthPlugin>,
    server_list_provider: Arc<dyn ServerListProvider>,
    auth_params: HashMap<String, String>,
    id: String,
    tasks: &TaskGroup,
) {
    info!("init auth task");
    let auth_context = Arc::new(AuthContext::default().add_params(auth_params));
    let server_list = server_list_provider.current_server_list().await;
    // First login
    auth_plugin
        .login(server_list.clone(), auth_context.clone())
        .in_current_span()
        .await;
    info!("init auth finish");

    tasks.spawn(
        async move {
            // Periodic refresh
            info!("auth plugin task start.");
            loop {
                let server_list = server_list_provider.current_server_list().await;
                auth_plugin
                    .login(server_list.clone(), auth_context.clone())
                    .in_current_span()
                    .await;
                debug!("auth_plugin schedule at fixed delay");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
        .instrument(debug_span!("auth_task", id = id)),
    );
}

#[derive(Debug, Default)]
pub struct RequestResource {
    pub request_type: String,
    pub namespace: Option<String>,
    pub group: Option<String>,
    pub resource: Option<String>,
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::common::remote::grpc::task_group::TaskOwner;
    use crate::common::remote::server_list::StaticServerListProvider;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::oneshot;

    struct BlockingRefresh {
        first_login: AtomicBool,
        started: Mutex<Option<oneshot::Sender<()>>>,
    }

    #[async_trait::async_trait]
    impl AuthPlugin for BlockingRefresh {
        async fn login(&self, _: Arc<Vec<String>>, _: Arc<AuthContext>) {
            if self.first_login.swap(false, Ordering::SeqCst) {
                return;
            }
            self.started
                .lock()
                .expect("test lock poisoned")
                .take()
                .expect("refresh must start once")
                .send(())
                .expect("test must receive start");
            std::future::pending::<()>().await;
        }

        fn get_login_identity(&self, _: RequestResource) -> LoginIdentityContext {
            LoginIdentityContext::default()
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_in_flight_auth_refresh() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let owner = TaskOwner::default();
            let (started_tx, started_rx) = oneshot::channel();
            let auth = Arc::new(BlockingRefresh {
                first_login: AtomicBool::new(true),
                started: Mutex::new(Some(started_tx)),
            });
            let provider = Arc::new(StaticServerListProvider::new(Vec::new()));
            init_auth_plugin(
                auth.clone(),
                provider.clone(),
                HashMap::new(),
                "shutdown-test".to_owned(),
                &owner.tasks,
            )
            .await;
            started_rx.await.expect("refresh must start");
            owner.tasks.shutdown().await;
            assert_eq!(
                Arc::strong_count(&auth),
                1,
                "refresh must release auth plugin"
            );
            assert_eq!(
                Arc::strong_count(&provider),
                1,
                "refresh must release provider"
            );
        })
        .await
        .expect("shutdown must cancel pending login without waiting for refresh interval");
    }
}
