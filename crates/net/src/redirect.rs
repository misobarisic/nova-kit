//! Redirect policy for clients that follow redirects.
use reqwest::redirect::{Attempt, Policy};

/// Follow at most `limit` redirects, and only to HTTPS URLs without embedded
/// credentials. `context` names the caller in the error message.
pub fn https_only(limit: usize, context: &'static str) -> Policy {
    Policy::custom(move |attempt| decide(attempt, limit, context))
}

fn decide(attempt: Attempt<'_>, limit: usize, context: &'static str) -> reqwest::redirect::Action {
    if attempt.previous().len() > limit {
        return attempt.error(format!("{context} redirect limit"));
    }
    let url = attempt.url();
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return attempt.error(format!(
            "{context} redirect requires HTTPS without credentials"
        ));
    }
    attempt.follow()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn a_redirect_to_cleartext_http_is_refused_with_the_callers_context() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request);
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{address}/next\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        let client = reqwest::blocking::Client::builder()
            .redirect(https_only(5, "fixture"))
            .build()
            .unwrap();
        let error = client.get(format!("http://{address}/")).send().unwrap_err();
        server.join().unwrap();
        assert!(error.is_redirect(), "{error:?}");
        let mut source: &dyn std::error::Error = &error;
        let mut chain = String::new();
        loop {
            chain.push_str(&source.to_string());
            match source.source() {
                Some(next) => source = next,
                None => break,
            }
        }
        assert!(
            chain.contains("fixture redirect requires HTTPS without credentials"),
            "{chain}"
        );
    }
}
