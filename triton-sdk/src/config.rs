use std::{collections::HashSet, time::Duration};

use solana_pubkey::Pubkey;

use crate::{account_sync::endpoint::GrpcEndpoint, error::ConfigError};

#[derive(Clone, Debug)]
pub struct AccountSyncConfig {
    pub endpoint: String,
    pub pinned_accounts: HashSet<Pubkey>,
    pub automatic_subscriptions: bool,
    pub cache_miss_wait: Duration,
    pub subscription_refresh: Duration,
    pub reconnect_min_delay: Duration,
    pub reconnect_max_delay: Duration,
    pub connect_timeout: Duration,
    pub close_timeout: Duration,
    pub dynamic_subscription_lifetime: Duration,
    pub http2_window_size: u32,
    pub max_decoded_message_size: usize,
    pub keepalive_interval: Duration,
    pub keepalive_timeout: Duration,
    pub keepalive_while_idle: bool,
}

impl Default for AccountSyncConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            pinned_accounts: HashSet::new(),
            automatic_subscriptions: true,
            cache_miss_wait: Duration::from_secs(5),
            subscription_refresh: Duration::from_secs(1),
            reconnect_min_delay: Duration::from_millis(100),
            reconnect_max_delay: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(5),
            dynamic_subscription_lifetime: Duration::from_secs(60),
            http2_window_size: 16 * 1024 * 1024,
            max_decoded_message_size: 16 * 1024 * 1024,
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            keepalive_while_idle: true,
        }
    }
}

impl AccountSyncConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        GrpcEndpoint::parse(&self.endpoint)?;
        for (name, duration) in [
            ("cache_miss_wait", self.cache_miss_wait),
            ("subscription_refresh", self.subscription_refresh),
            ("reconnect_min_delay", self.reconnect_min_delay),
            ("reconnect_max_delay", self.reconnect_max_delay),
            ("connect_timeout", self.connect_timeout),
            ("close_timeout", self.close_timeout),
            (
                "dynamic_subscription_lifetime",
                self.dynamic_subscription_lifetime,
            ),
            ("keepalive_interval", self.keepalive_interval),
            ("keepalive_timeout", self.keepalive_timeout),
        ] {
            if duration.is_zero() {
                return Err(ConfigError::Zero(name));
            }
        }
        if self.reconnect_min_delay > self.reconnect_max_delay {
            return Err(ConfigError::ReconnectRange);
        }
        if self.http2_window_size == 0 {
            return Err(ConfigError::Zero("http2_window_size"));
        }
        if self.max_decoded_message_size == 0 {
            return Err(ConfigError::Zero("max_decoded_message_size"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_defaults_after_setting_endpoint() {
        let config = AccountSyncConfig {
            endpoint: "https://example.com".into(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.cache_miss_wait, Duration::from_secs(5));
    }

    #[test]
    fn validates_url_and_authentication_before_starting_transport() {
        for endpoint in ["example.com/token", "localhost:10000/token"] {
            assert!(
                AccountSyncConfig {
                    endpoint: endpoint.into(),
                    ..Default::default()
                }
                .validate()
                .is_ok()
            );
        }
        let config = AccountSyncConfig {
            endpoint: "https://example.com/token/extra".into(),
            ..Default::default()
        };
        assert!(matches!(config.validate(), Err(ConfigError::EndpointPath)));
    }

    #[test]
    fn rejects_bad_endpoint_and_zero_limit() {
        let mut config = AccountSyncConfig::default();
        assert!(matches!(
            config.validate(),
            Err(ConfigError::EndpointUrl(_))
        ));
        config.endpoint = "http://example.com".into();
        config.max_decoded_message_size = 0;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::Zero("max_decoded_message_size"))
        ));
    }
}
