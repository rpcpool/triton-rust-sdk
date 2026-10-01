use tonic::{
    metadata::MetadataValue,
    transport::{ClientTlsConfig, Endpoint},
};
use url::Url;

use crate::error::ConfigError;

pub(crate) struct GrpcEndpoint {
    pub endpoint: Endpoint,
    pub token: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl GrpcEndpoint {
    pub fn parse(endpoint: &str) -> Result<Self, ConfigError> {
        let is_local = ["localhost", "127.0.0.1", "[::1]", "::1", "0.0.0.0"]
            .iter()
            .any(|prefix| endpoint.starts_with(prefix));
        let scheme = if is_local { "http" } else { "https" };
        let url = if endpoint.contains("://") {
            Url::parse(endpoint)
        } else {
            Url::parse(&format!("{scheme}://{endpoint}"))
        }
        .map_err(ConfigError::EndpointUrl)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ConfigError::EndpointScheme);
        }

        let mut segments = url.path().split('/').filter(|segment| !segment.is_empty());
        let token = segments.next();
        if segments.next().is_some() {
            return Err(ConfigError::EndpointPath);
        }
        let token = token
            .map(|token| {
                let mut value =
                    MetadataValue::try_from(token).map_err(|_| ConfigError::EndpointToken)?;
                value.set_sensitive(true);
                Ok(value)
            })
            .transpose()?;
        let mut endpoint = Endpoint::from_shared(url.origin().ascii_serialization())
            .map_err(ConfigError::Endpoint)?;
        if url.scheme() == "https" {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map_err(ConfigError::Endpoint)?;
        }
        Ok(Self { endpoint, token })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_js_url_and_token_rules() {
        for (input, origin, token) in [
            (
                "https://example.com/token",
                "https://example.com/",
                Some("token"),
            ),
            ("example.com/token", "https://example.com/", Some("token")),
            (
                "localhost:10000/token",
                "http://localhost:10000/",
                Some("token"),
            ),
            ("127.0.0.1:10000", "http://127.0.0.1:10000/", None),
            ("[::1]:10000/token", "http://[::1]:10000/", Some("token")),
            ("0.0.0.0:10000", "http://0.0.0.0:10000/", None),
            (
                "http://example.com:11000",
                "http://example.com:11000/",
                None,
            ),
            (
                "https://localhost:11000/token",
                "https://localhost:11000/",
                Some("token"),
            ),
            (
                "https://example.com:8443//token///?ignored=value#fragment",
                "https://example.com:8443/",
                Some("token"),
            ),
            (
                "https://example.com///?token=ignored#fragment",
                "https://example.com/",
                None,
            ),
            (
                "https://example.com/a%2Fb%20c",
                "https://example.com/",
                Some("a%2Fb%20c"),
            ),
            (
                "https://example.com/é",
                "https://example.com/",
                Some("%C3%A9"),
            ),
            (
                "https://user:password@example.com/token",
                "https://example.com/",
                Some("token"),
            ),
        ] {
            let parsed = GrpcEndpoint::parse(input).unwrap();
            assert_eq!(parsed.endpoint.uri().to_string(), origin, "{input}");
            assert_eq!(
                parsed.token.as_ref().map(|value| value.to_str().unwrap()),
                token,
                "{input}"
            );
            assert!(
                parsed
                    .token
                    .as_ref()
                    .is_none_or(|value| value.is_sensitive())
            );
        }
    }

    #[test]
    fn rejects_invalid_urls_without_exposing_tokens() {
        for input in [
            "",
            "https://",
            "https://bad host/secret-token",
            "https://example.com:99999/secret-token",
            "ws://example.com/secret-token",
            "ftp://example.com/secret-token",
            "https://example.com/secret-token/extra",
            "https://example.com/YellowstoneAccountSyncGrpcService/Subscribe",
        ] {
            let Err(error) = GrpcEndpoint::parse(input) else {
                panic!("invalid endpoint accepted: {input}");
            };
            assert!(!format!("{error} {error:?}").contains("secret-token"));
        }
        assert!(matches!(
            GrpcEndpoint::parse("ws://example.com/token"),
            Err(ConfigError::EndpointScheme)
        ));
        assert!(matches!(
            GrpcEndpoint::parse("https://example.com/token/extra"),
            Err(ConfigError::EndpointPath)
        ));
    }
}
