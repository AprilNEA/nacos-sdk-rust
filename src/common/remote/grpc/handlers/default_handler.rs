use crate::{
    common::remote::grpc::nacos_grpc_service::ServerRequestHandler, nacos_proto::v2::Payload,
};
use async_trait::async_trait;
use tracing::info;

pub(crate) struct DefaultHandler;

#[async_trait]
impl ServerRequestHandler for DefaultHandler {
    async fn request_reply(&self, _request: Payload) -> Option<Payload> {
        info!("unknown gRPC server request ignored");
        None
    }
}
