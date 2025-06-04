// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::credentials::errors::{self, CredentialsError};
use base64::Engine;
use serde::Deserialize;
use std::collections::HashMap;

type Result<T> = std::result::Result<T, CredentialsError>;

/// Token Exchange grant type for a sts exchange.
pub const TOKEN_EXCHANGE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
/// TokenType for a sts exchange.
pub const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
/// JWT TokenType for a sts exchange.
pub const JWT_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:jwt";

/// Handles OAuth2 Secure Token Service (STS) exchange.
/// Reference: https://datatracker.ietf.org/doc/html/rfc8693
pub struct STSHandler {}

impl STSHandler {
    /// Performs an oauth2 token exchange with the provided [ExchangeTokenRequest] information.
    pub(crate) async fn exchange_token(req: ExchangeTokenRequest) -> Result<TokenResponse> {
        let mut params = HashMap::new();

        params.insert("grant_type", TOKEN_EXCHANGE_GRANT_TYPE.to_string());
        params.insert("requested_token_type", ACCESS_TOKEN_TYPE.to_string());

        params.insert("subject_token", req.subject_token);
        params.insert("subject_token_type", req.subject_token_type);

        if !req.scope.is_empty() {
            params.insert("scope", req.scope.join(" "));
        }

        if let Some(audience) = req.audience {
            params.insert("audience", audience);
        }
        if let Some(resource) = req.resource {
            params.insert("resource", resource);
        }
        if let Some(actor_token) = req.actor_token {
            params.insert("actor_token", actor_token);
        }
        if let Some(actor_token_type) = req.actor_token_type {
            params.insert("actor_token_type", actor_token_type);
        }

        if let Some(options) = req.extra_options {
            if let Ok(value) = serde_json::to_value(options) {
                params.insert("options", value.to_string());
            }
        }

        Self::execute(req.url, req.authentication, req.headers, params).await
    }

    /// Execute http request and token exchange
    async fn execute(
        url: String,
        client_auth: ClientAuthentication,
        headers: http::HeaderMap,
        params: HashMap<&str, String>,
    ) -> Result<TokenResponse> {
        let client = reqwest::Client::new();

        let mut headers = headers.clone();
        client_auth.inject_auth(&mut headers)?;

        let res = client
            .post(url)
            .form(&params)
            .headers(headers)
            .send()
            .await
            .map_err(|e| errors::from_http_error(e, MSG))?;

        let status = res.status();
        if !status.is_success() {
            let err = errors::from_http_response(res, MSG).await;
            return Err(err);
        }
        let token_res = res
            .json::<TokenResponse>()
            .await
            .map_err(|err| CredentialsError::from_source(false, err))?;
        Ok(token_res)
    }
}

const MSG: &str = "failed to exchange token";

/// TokenResponse is used to decode the remote server response during
/// an oauth2 token exchange.
#[derive(Deserialize, Default, PartialEq, Debug)]
pub struct TokenResponse {
    pub access_token: String,
    pub issued_token_type: String,
    pub token_type: String,
    pub expires_in: u64,
    pub scope: Option<String>,
    pub refresh_token: Option<String>,
}

/// ClientAuthentication represents an OAuth client ID and secret and the
/// mechanism for passing these credentials as stated
/// in https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1.
/// Only authentication via headers is currently supported.
#[derive(Clone, Debug, Default)]
pub struct ClientAuthentication {
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
}

impl ClientAuthentication {
    /// Add authentication to a Secure Token Service exchange request.
    fn inject_auth(&self, headers: &mut http::HeaderMap) -> Result<()> {
        if let (Some(client_id), Some(client_secret)) =
            (self.client_id.clone(), self.client_secret.clone())
        {
            let plain_header = format!("{}:{}", client_id, client_secret);
            let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(plain_header);
            let header = http::HeaderValue::from_str(format!("Basic {encoded}").as_str());
            if let Ok(value) = header {
                headers.insert("Authorization", value);
            }
        }
        Ok(())
    }
}

/// Information required to perform an oauth2 token exchange with the provided endpoint.
#[derive(Default)]
pub struct ExchangeTokenRequest {
    pub url: String,
    pub authentication: ClientAuthentication,
    pub headers: http::HeaderMap,
    pub resource: Option<String>,
    pub subject_token: String,
    pub subject_token_type: String,
    pub audience: Option<String>,
    pub scope: Vec<String>,
    pub actor_token: Option<String>,
    pub actor_token_type: Option<String>,
    pub extra_options: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::constants::DEFAULT_SCOPE;
    use http::StatusCode;
    use serde_json::json;
    use std::error::Error as _;
    use url::form_urlencoded;
    use wiremock::{Match, Mock, MockServer, Request, ResponseTemplate, matchers::*};

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    struct FormUrlEncodedContainsMatcher(String, String);

    fn form_url_encoded_contains<K, V>(key: K, value: V) -> FormUrlEncodedContainsMatcher
    where
        K: Into<String>,
        V: Into<String>,
    {
        FormUrlEncodedContainsMatcher(key.into(), value.into())
    }

    impl Match for FormUrlEncodedContainsMatcher {
        fn matches(&self, request: &Request) -> bool {
            form_urlencoded::parse(request.body.as_ref())
                .into_owned()
                .any(|(k, v)| k == self.0 && v == self.1)
        }
    }

    #[tokio::test]
    async fn exchange_token() -> TestResult {
        let authentication = ClientAuthentication {
            client_id: Some("client_id".to_string()),
            client_secret: Some("supersecret".to_string()),
        };
        let response_body = ResponseTemplate::new(200).set_body_json(json!({
            "access_token":"an_example_token",
            "issued_token_type":"urn:ietf:params:oauth:token-type:access_token",
            "token_type":"Bearer",
            "expires_in":3600,
            "scope":DEFAULT_SCOPE
        }));

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/sts"))
            .and(form_url_encoded_contains(
                "grant_type",
                TOKEN_EXCHANGE_GRANT_TYPE,
            ))
            .and(form_url_encoded_contains(
                "subject_token",
                "an_example_token",
            ))
            .and(form_url_encoded_contains(
                "requested_token_type",
                ACCESS_TOKEN_TYPE,
            ))
            .and(form_url_encoded_contains(
                "subject_token_type",
                JWT_TOKEN_TYPE,
            ))
            .and(form_url_encoded_contains(
                "audience",
                "32555940559.apps.googleusercontent.com",
            ))
            .and(form_url_encoded_contains("scope", DEFAULT_SCOPE))
            .and(basic_auth("client_id", "supersecret"))
            .and(header("content-type", "application/x-www-form-urlencoded"))
            .respond_with(response_body)
            .mount(&server)
            .await;

        let url = format!("{}/sts", server.uri()).to_string();
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let token_req = ExchangeTokenRequest {
            url,
            headers,
            authentication,
            audience: Some("32555940559.apps.googleusercontent.com".to_string()),
            scope: [DEFAULT_SCOPE.to_string()].to_vec(),
            subject_token: "an_example_token".to_string(),
            subject_token_type: JWT_TOKEN_TYPE.to_string(),
            ..ExchangeTokenRequest::default()
        };
        let resp = STSHandler::exchange_token(token_req).await?;

        assert_eq!(
            resp,
            TokenResponse {
                access_token: "an_example_token".to_string(),
                refresh_token: None,
                issued_token_type: ACCESS_TOKEN_TYPE.to_string(),
                token_type: "Bearer".to_string(),
                expires_in: 3600,
                scope: Some(DEFAULT_SCOPE.to_string()),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn exchange_token_err() -> TestResult {
        let authentication = ClientAuthentication {
            client_id: Some("client_id".to_string()),
            client_secret: Some("supersecret".to_string()),
        };

        let response_body = ResponseTemplate::new(400).set_body_json(json!({
            "error":"bad request",
        }));

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/fail"))
            .and(form_url_encoded_contains(
                "subject_token",
                "an_example_token",
            ))
            .and(form_url_encoded_contains(
                "subject_token_type",
                JWT_TOKEN_TYPE,
            ))
            .and(form_url_encoded_contains(
                "grant_type",
                TOKEN_EXCHANGE_GRANT_TYPE,
            ))
            .and(form_url_encoded_contains(
                "requested_token_type",
                ACCESS_TOKEN_TYPE,
            ))
            .and(basic_auth("client_id", "supersecret"))
            .and(header("content-type", "application/x-www-form-urlencoded"))
            .respond_with(response_body)
            .mount(&server)
            .await;

        let url = format!("{}/fail", server.uri()).to_string();
        let headers = http::HeaderMap::new();
        let token_req = ExchangeTokenRequest {
            url,
            headers,
            authentication,
            subject_token: "an_example_token".to_string(),
            subject_token_type: JWT_TOKEN_TYPE.to_string(),
            ..ExchangeTokenRequest::default()
        };
        let err = STSHandler::exchange_token(token_req).await.unwrap_err();
        assert!(!err.is_transient(), "{err:?}");
        assert!(err.to_string().contains(MSG), "{err}, debug={err:?}");
        assert!(
            err.to_string().contains("bad request"),
            "{err}, debug={err:?}"
        );
        let source = err
            .source()
            .and_then(|e| e.downcast_ref::<reqwest::Error>());
        assert!(
            matches!(source, Some(e) if e.status() == Some(StatusCode::BAD_REQUEST)),
            "{err:?}"
        );

        Ok(())
    }
}
