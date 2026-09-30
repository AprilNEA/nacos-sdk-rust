use prost_types::Any;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::fmt::Debug;
use tracing::{error, warn};

use crate::api::error::Error::{ErrResponse, ErrResult, Serialization};
use crate::api::error::Result;
use crate::api::plugin::RequestResource;
use crate::common::remote::grpc::message::response::ErrorResponse;
use crate::nacos_proto::v2::{Metadata, Payload};

pub(crate) mod request;
pub(crate) mod response;

#[derive(Debug)]
pub(crate) struct GrpcMessage<T>
where
    T: GrpcMessageData,
{
    headers: HashMap<String, String>,
    body: T,
    client_ip: String,
}

impl<T> GrpcMessage<T>
where
    T: GrpcMessageData,
{
    #[allow(dead_code)]
    pub(crate) fn body(&self) -> &T {
        &self.body
    }

    pub(crate) fn into_body(self) -> T {
        self.body
    }

    pub(crate) fn into_payload(self) -> Result<Payload> {
        let meta_data = Metadata {
            r#type: T::identity().to_string(),
            client_ip: self.client_ip.to_string(),
            headers: self.headers,
        };

        let body = match self.body.to_proto_any() {
            Ok(proto_any) => proto_any,
            Err(error) => {
                error!(target_type = %T::identity(), "gRPC message serialization failed");
                return Err(error);
            }
        };

        let payload = Payload {
            metadata: Some(meta_data),
            body: Some(body),
        };
        Ok(payload)
    }

    pub(crate) fn from_payload(payload: Payload) -> Result<Self> {
        let body_any = match payload.body {
            Some(body) => body,
            None => return Err(ErrResult("grpc payload body is empty".to_string())),
        };

        let meta_data = payload.metadata.unwrap_or_default();
        let r_type = meta_data.r#type;
        let client_ip = meta_data.client_ip;
        let headers = meta_data.headers;

        // try to serialize target type if r_type is not empty
        if !r_type.is_empty() {
            if T::identity().eq(&r_type) {
                let de_body = T::from_proto_any(&body_any)?;
                return Ok(GrpcMessage {
                    headers,
                    body: de_body,
                    client_ip,
                });
            }

            // type mismatch - try to convert to ErrorResponse
            warn!(
                target_type = %T::identity(),
                "gRPC payload type mismatch; trying ErrorResponse"
            );
            let error_response: ErrorResponse = ErrorResponse::from_proto_any(&body_any)?;
            return Err(ErrResponse(
                error_response.request_id,
                error_response.result_code,
                error_response.error_code,
                error_response.message,
            ));
        }

        // empty r_type - try direct conversion
        warn!("payload type is empty!");
        let ret: Result<T> = T::from_proto_any(&body_any);
        if let Ok(de_body) = ret {
            return Ok(GrpcMessage {
                headers,
                body: de_body,
                client_ip,
            });
        }

        // A server can omit the message type for ErrorResponse.
        let error = ret.unwrap_err();

        let ret: Result<ErrorResponse> = ErrorResponse::from_proto_any(&body_any);
        if let Ok(error_response) = ret {
            return Err(ErrResponse(
                error_response.request_id,
                error_response.result_code,
                error_response.error_code,
                error_response.message,
            ));
        }

        // both conversions failed - return original error
        error!("gRPC payload cannot be decoded as ErrorResponse");
        Err(error)
    }
}

pub(crate) trait GrpcMessageData:
    Debug + Clone + Serialize + DeserializeOwned + Send
{
    fn identity<'a>() -> std::borrow::Cow<'a, str>;

    fn to_proto_any(&self) -> Result<Any> {
        let byte_data = match serde_json::to_vec(self) {
            Ok(data) => data,
            Err(error) => {
                return Err(Serialization(error));
            }
        };
        let any = Any {
            type_url: Self::identity().to_string(),
            value: byte_data,
        };
        Ok(any)
    }

    fn from_proto_any<T: GrpcMessageData>(any: &Any) -> Result<T> {
        let body = match serde_json::from_slice(&any.value) {
            Ok(data) => data,
            Err(error) => {
                error!(target_type = %T::identity(), category = ?error.classify(), line = error.line(), column = error.column(), "gRPC payload decoding failed");
                return Err(Serialization(error));
            }
        };
        Ok(body)
    }
}

#[allow(dead_code)]
pub(crate) trait GrpcRequestMessage: GrpcMessageData {
    fn header(&self, key: &str) -> Option<&String>;

    fn headers(&self) -> &HashMap<String, String>;

    fn take_headers(&mut self) -> HashMap<String, String>;

    fn add_headers(&mut self, map: HashMap<String, String>);

    fn request_id(&self) -> Option<&String>;

    fn module(&self) -> &str;

    fn request_resource(&self) -> Option<RequestResource>;
}

#[allow(dead_code)]
pub(crate) trait GrpcResponseMessage: GrpcMessageData {
    fn request_id(&self) -> Option<&String>;

    fn result_code(&self) -> i32;

    fn error_code(&self) -> i32;

    fn message(&self) -> Option<&String>;

    fn is_success(&self) -> bool;
}

pub(crate) struct GrpcMessageBuilder<T>
where
    T: GrpcMessageData,
{
    headers: HashMap<String, String>,
    body: T,
    client_ip: String,
}

impl<T> GrpcMessageBuilder<T>
where
    T: GrpcMessageData,
{
    pub(crate) fn new(body: T) -> Self {
        GrpcMessageBuilder {
            headers: HashMap::<String, String>::new(),
            body,
            client_ip: crate::common::util::LOCAL_IP.to_owned(),
        }
    }

    pub(crate) fn header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    pub(crate) fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers.extend(headers);
        self
    }

    pub(crate) fn build(self) -> GrpcMessage<T> {
        GrpcMessage {
            headers: self.headers,
            body: self.body,
            client_ip: self.client_ip,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::remote::grpc::handlers::default_handler::DefaultHandler;
    use crate::common::remote::grpc::nacos_grpc_service::ServerRequestHandler;
    use std::sync::Arc;
    use tracing::instrument::WithSubscriber;

    #[tokio::test]
    async fn malformed_and_unknown_payload_logs_exclude_body_and_headers() {
        let log_path =
            std::env::temp_dir().join(format!("nacos-grpc-log-{}", rand::random::<u64>()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(Arc::new(
                std::fs::File::create(&log_path).expect("create gRPC payload test log"),
            ))
            .finish();
        async {
            for message_type in ["ErrorResponse", "", "type-sentinel"] {
                let payload = Payload {
                    metadata: Some(Metadata {
                        r#type: message_type.to_string(),
                        client_ip: "client-sentinel".to_string(),
                        headers: HashMap::from([("accessToken".to_string(), "token-sentinel".to_string())]),
                    }),
                    body: Some(Any {
                        type_url: "type-sentinel".to_string(),
                        value: br#"{"resultCode":"token-sentinel","content":"password: yaml-sentinel"}"#.to_vec(),
                    }),
                };
                let error = GrpcMessage::<ErrorResponse>::from_payload(payload.clone()).expect_err("malformed server response must fail decoding");
                assert!(crate::common::remote::grpc::utils::convert::<()>(Err(error), "config response conversion failed").is_err());
                assert!(DefaultHandler.request_reply(payload).await.is_none());
            }
        }.with_subscriber(subscriber).await;

        let logs = std::fs::read_to_string(&log_path).expect("read gRPC payload test log");
        for secret in [
            "yaml-sentinel",
            "token-sentinel",
            "type-sentinel",
            "client-sentinel",
        ] {
            assert!(
                !logs.contains(secret),
                "gRPC logs must exclude response content and credentials"
            );
        }
        assert!(logs.contains("gRPC payload decoding failed"));
        assert!(logs.contains("unknown gRPC server request ignored"));
        std::fs::remove_file(log_path).expect("remove gRPC payload test log");
    }
}
