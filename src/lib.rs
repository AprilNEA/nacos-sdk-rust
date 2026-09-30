// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#![deny(unsafe_code)]
#![deny(rust_2018_idioms, clippy::disallowed_methods, clippy::disallowed_types)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

//! # Nacos in Rust
//!
//! Thorough examples have been provided in our [repository](https://github.com/nacos-group/nacos-sdk-rust).
//!
//! ## Add Dependency
//!
//! Add the dependency in `Cargo.toml`:
//! ```toml
//! [dependencies]
//! nacos-sdk = { version = "0.8", features = ["default"] }
//! ```
//!
//! ## General Configurations and Initialization
//!
//! Nacos needs to be initialized. Please see the `api` module.
//!
//! ### Example of Config
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let config_service = nacos_sdk::api::config::ConfigServiceBuilder::new(
//!       nacos_sdk::api::props::ClientProps::new()
//!          .server_addr("127.0.0.1:8848")
//!          // Attention! "public" is "", it is recommended to customize the namespace with clear meaning.
//!          .namespace("")
//!          .app_name("todo-your-app-name"),
//!  )
//!  .build()
//!  .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Example of Naming
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let naming_service = nacos_sdk::api::naming::NamingServiceBuilder::new(
//!       nacos_sdk::api::props::ClientProps::new()
//!          .server_addr("127.0.0.1:8848")
//!          // Attention! "public" is "", it is recommended to customize the namespace with clear meaning.
//!          .namespace("")
//!          .app_name("todo-your-app-name"),
//!  )
//!  .build()
//!  .await?;
//! # Ok(())
//! # }
//! ```
//!

/// Nacos API
pub mod api;

mod common;
#[cfg(feature = "config")]
mod config;
#[cfg(feature = "naming")]
mod naming;

#[allow(dead_code)]
#[path = ""]
mod nacos_proto {
    #[path = "_.rs"]
    pub mod v2;
}

use crate::api::constants::ENV_NACOS_CLIENT_PROPS_FILE_PATH;
use std::collections::HashMap;

static PROPERTIES: std::sync::LazyLock<Result<HashMap<String, String>, &'static str>> =
    std::sync::LazyLock::new(properties::load);

pub(crate) mod properties {
    use crate::{ENV_NACOS_CLIENT_PROPS_FILE_PATH, PROPERTIES};
    use std::collections::HashMap;

    pub(crate) fn init() -> crate::api::error::Result<()> {
        PROPERTIES.as_ref().map(|_| ()).map_err(|message| {
            crate::api::error::Error::InvalidParam(
                ENV_NACOS_CLIENT_PROPS_FILE_PATH.to_string(),
                (*message).to_string(),
            )
        })
    }

    pub(super) fn load() -> Result<HashMap<String, String>, &'static str> {
        let mut properties = std::env::vars_os()
            .map(|(key, value)| {
                Ok((
                    key.into_string()
                        .map_err(|_| "environment variable name is not valid Unicode")?,
                    value
                        .into_string()
                        .map_err(|_| "environment variable value is not valid Unicode")?,
                ))
            })
            .collect::<Result<HashMap<_, _>, &'static str>>()?;
        if let Some(path) = properties.get(ENV_NACOS_CLIENT_PROPS_FILE_PATH) {
            let entries = dotenvy::from_path_iter(path)
                .map_err(|_| "cannot read explicit properties file")?;
            merge_file(&mut properties, entries, "invalid explicit properties file")?;
        }
        match dotenvy::dotenv_iter() {
            Ok(entries) => merge_file(&mut properties, entries, "invalid .env file")?,
            Err(error) if error.not_found() => {}
            Err(_) => return Err("cannot read .env file"),
        }
        Ok(properties)
    }

    fn merge_file(
        properties: &mut HashMap<String, String>,
        entries: dotenvy::Iter<std::fs::File>,
        error_message: &'static str,
    ) -> Result<(), &'static str> {
        for entry in entries {
            // Dotenv parse errors contain the complete line, including credentials.
            let (key, value) = entry.map_err(|_| error_message)?;
            properties.entry(key).or_insert(value);
        }
        Ok(())
    }

    fn values() -> &'static HashMap<String, String> {
        // Client builders validate initialization before reading any properties.
        PROPERTIES
            .as_ref()
            .expect("properties initialization must succeed before reading settings")
    }

    pub(crate) fn get_value_option<Key>(key: Key) -> Option<String>
    where
        Key: AsRef<str>,
    {
        values().get(key.as_ref()).cloned()
    }

    pub(crate) fn get_value<Key, Default>(key: Key, default: Default) -> String
    where
        Key: AsRef<str>,
        Default: AsRef<str>,
    {
        values()
            .get(key.as_ref())
            .map_or(default.as_ref().to_string(), |value| value.to_string())
    }

    pub(crate) fn get_value_u16<Key>(key: Key, default: u16) -> u16
    where
        Key: AsRef<str>,
    {
        values().get(key.as_ref()).map_or(default, |value| {
            value.to_string().parse::<u16>().unwrap_or(default)
        })
    }

    pub(crate) fn get_value_bool<Key>(key: Key, default: bool) -> bool
    where
        Key: AsRef<str>,
    {
        values().get(key.as_ref()).map_or(default, |value| {
            value.to_string().parse::<bool>().unwrap_or(default)
        })
    }
}

#[cfg(test)]
mod tests {
    use prost_types::Any;
    use std::collections::HashMap;

    use crate::nacos_proto::v2::Metadata;
    use crate::nacos_proto::v2::Payload;

    #[test]
    fn it_works_nacos_proto() {
        let body = Any {
            type_url: String::new(),
            value: Vec::from("{\"cluster\":\"DEFAULT\",\"healthyOnly\":true}"),
        };
        let metadata = Metadata {
            r#type: String::from("com.alibaba.nacos.api.naming.remote.request.ServiceQueryRequest"),
            client_ip: String::from("127.0.0.1"),
            headers: HashMap::new(),
        };
        let payload = Payload {
            metadata: Some(metadata),
            body: Some(body),
        };
        // println!("{:?}", payload);
        assert_eq!(
            payload
                .metadata
                .expect("Metadata should exist after checking it's some")
                .r#type,
            "com.alibaba.nacos.api.naming.remote.request.ServiceQueryRequest"
        );
        assert_eq!(
            payload
                .body
                .expect("Body should exist after checking it's some")
                .value,
            Vec::from("{\"cluster\":\"DEFAULT\",\"healthyOnly\":true}")
        );
    }
}

#[cfg(test)]
mod test_props {
    use crate::api::constants::ENV_NACOS_CLIENT_NAMING_PUSH_EMPTY_PROTECTION;
    use crate::properties::{get_value, get_value_bool, get_value_u16};

    #[test]
    fn properties_files_preserve_environment_and_report_invalid_files() {
        const CHILD_ENV: &str = "NACOS_PROPERTIES_TEST_CHILD";
        if let Ok(mode) = std::env::var(CHILD_ENV) {
            let initialized = crate::properties::init();
            if mode == "valid" {
                initialized.expect("valid properties files must initialize successfully");
                assert_eq!(get_value("SDK_ENV_PRIORITY", ""), "process");
                assert_eq!(get_value("SDK_FILE_PRIORITY", ""), "explicit");
                assert_eq!(get_value("SDK_FILE_ONLY", ""), "first");
                assert_eq!(get_value("SDK_DOTENV_ONLY", ""), "default");
            } else if mode == "nonunicode" {
                let error = initialized
                    .expect_err("non-Unicode environment values must fail initialization");
                assert!(error.to_string().contains("not valid Unicode"));
                assert!(!format!("{error:?}").contains("credential-sentinel"));
            } else {
                let error = initialized
                    .expect_err("invalid or missing explicit properties files must fail");
                assert!(error.to_string().contains("explicit properties file"));
                assert!(!format!("{error:?}").contains("credential-sentinel"));
            }
            for key in ["SDK_FILE_PRIORITY", "SDK_FILE_ONLY", "SDK_DOTENV_ONLY"] {
                assert!(std::env::var_os(key).is_none());
            }
            return;
        }

        let directory =
            std::env::temp_dir().join(format!("nacos-properties-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&directory).expect("create isolated properties test directory");
        std::fs::write(
            directory.join(".env"),
            "SDK_ENV_PRIORITY=default\nSDK_FILE_PRIORITY=default\nSDK_DOTENV_ONLY=default\n",
        )
        .expect("write fallback properties fixture");
        std::fs::write(directory.join("valid.env"), "SDK_ENV_PRIORITY=explicit\nSDK_FILE_PRIORITY=explicit\nSDK_FILE_ONLY=first\nSDK_FILE_ONLY=second\n").expect("write valid explicit properties fixture");
        std::fs::write(
            directory.join("invalid.env"),
            "SDK_FILE_ONLY=\"credential-sentinel\n",
        )
        .expect("write invalid explicit properties fixture");
        for mode in ["valid", "invalid", "missing"]
            .into_iter()
            .chain(cfg!(unix).then_some("nonunicode"))
        {
            let mut command = std::process::Command::new(
                std::env::current_exe().expect("locate properties test executable"),
            );
            command
                .args([
                    "--exact",
                    "test_props::properties_files_preserve_environment_and_report_invalid_files",
                ])
                .current_dir(&directory)
                .env(CHILD_ENV, mode)
                .env(
                    crate::ENV_NACOS_CLIENT_PROPS_FILE_PATH,
                    format!("{mode}.env"),
                )
                .env("SDK_ENV_PRIORITY", "process")
                .env_remove("SDK_FILE_PRIORITY")
                .env_remove("SDK_FILE_ONLY")
                .env_remove("SDK_DOTENV_ONLY");
            #[cfg(unix)]
            if mode == "nonunicode" {
                use std::os::unix::ffi::OsStringExt;
                command.env(
                    "SDK_NON_UNICODE",
                    std::ffi::OsString::from_vec(b"credential-sentinel\xff".to_vec()),
                );
            }
            let status = command
                .status()
                .expect("run isolated properties test process");
            assert!(status.success());
        }
        std::fs::remove_dir_all(directory).expect("remove properties test directory");
    }

    #[test]
    fn test_get_value() {
        let v = get_value("ENV_TEST", "TEST");
        assert_eq!(v, "TEST");
    }

    #[test]
    fn test_get_value_bool() {
        let v = get_value_bool(ENV_NACOS_CLIENT_NAMING_PUSH_EMPTY_PROTECTION, true);
        assert!(v);
    }

    #[test]
    fn test_get_value_u16() {
        let not_exist_key = "MUST_NOT_EXIST";
        let v = get_value_u16(not_exist_key, 91);
        assert_eq!(v, 91);
    }
}

#[cfg(test)]
mod test_config {
    use std::sync::Once;

    use tracing::metadata::LevelFilter;

    static LOGGER_INIT: Once = Once::new();

    pub(crate) fn setup_log() {
        LOGGER_INIT.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_thread_names(true)
                .with_file(true)
                .with_level(true)
                .with_line_number(true)
                .with_thread_ids(true)
                .with_max_level(LevelFilter::INFO)
                .try_init();
        });
    }
}
