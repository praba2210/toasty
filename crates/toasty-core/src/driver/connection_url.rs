use crate::{Error, Result};
use fluent_uri::IriRef;
use percent_encoding::percent_decode_str;
use std::{borrow::Cow, ops::Range, path::PathBuf};

/// A parsed database connection URL.
///
/// Connection URLs use the common `<scheme>:<target>?<query>` form, but the
/// target is interpreted by the selected database driver. Network drivers use
/// the conventional `//user:password@host:port/path` target. File-backed
/// drivers use [`file_path`](Self::file_path), which treats `//` as an optional
/// marker and keeps the following text as the path.
///
/// This distinction allows connection strings such as `sqlite://todos.db` and
/// `sqlite://:memory:` without interpreting `todos.db` or `:memory:` as a host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionUrl<'a> {
    value: &'a str,
    scheme_end: usize,
}

impl<'a> ConnectionUrl<'a> {
    /// Parses a database connection URL.
    ///
    /// This validates the scheme. Drivers validate the target and query
    /// parameters they support.
    pub fn parse(value: &'a str) -> Result<Self> {
        let scheme_end = value.find(':').ok_or_else(|| {
            Error::invalid_connection_url(format!("connection URL has no scheme; url={value}"))
        })?;
        let scheme = &value[..scheme_end];

        if !is_valid_scheme(scheme) {
            return Err(Error::invalid_connection_url(format!(
                "connection URL has an invalid scheme; url={value}"
            )));
        }

        Ok(Self { value, scheme_end })
    }

    /// Returns the connection URL exactly as supplied.
    pub fn as_str(&self) -> &'a str {
        self.value
    }

    /// Returns the URL scheme without the trailing colon.
    pub fn scheme(&self) -> &'a str {
        &self.value[..self.scheme_end]
    }

    /// Returns `true` when the URL uses `expected`.
    ///
    /// URL schemes are ASCII case-insensitive.
    pub fn has_scheme(&self, expected: &str) -> bool {
        self.scheme().eq_ignore_ascii_case(expected)
    }

    /// Returns the path component of an authority-based connection URL.
    ///
    /// For `postgresql://localhost/mydb`, this returns `/mydb`. Use
    /// [`file_path`](Self::file_path) for SQLite and other file-backed drivers.
    pub fn path(&self) -> &'a str {
        let rest = self.rest();
        let path = if let Some(authority) = self.authority_range() {
            &rest[authority.end..]
        } else {
            rest
        };

        split_before(path, &['?', '#'])
    }

    /// Returns the percent-decoded path component.
    pub fn decoded_path(&self) -> Result<Cow<'a, str>> {
        percent_decode_str(self.path())
            .decode_utf8()
            .map_err(|_| Error::invalid_connection_url("URL path is not valid UTF-8"))
    }

    /// Returns the file path named by a file-backed connection URL.
    ///
    /// The optional `//` after the scheme is discarded, a query string is not
    /// part of the path, and `#` remains a normal filename character. The path
    /// is percent-decoded before it is returned.
    pub fn file_path(&self) -> Result<PathBuf> {
        let rest = self.rest();
        let path = rest.strip_prefix("//").unwrap_or(rest);
        let path = split_before(path, &['?']);

        if path.is_empty() {
            return Err(Error::invalid_connection_url(format!(
                "connection URL does not name a database file; url={}",
                self.value
            )));
        }

        Ok(PathBuf::from(
            percent_decode_str(path).decode_utf8_lossy().as_ref(),
        ))
    }

    /// Returns the percent-decoded username from the authority component.
    pub fn username(&self) -> Result<Option<Cow<'a, str>>> {
        let Some(userinfo) = self.userinfo() else {
            return Ok(None);
        };
        let username = userinfo.split_once(':').map_or(userinfo, |(name, _)| name);
        percent_decode_str(username)
            .decode_utf8()
            .map(Some)
            .map_err(|_| Error::invalid_connection_url("username is not valid UTF-8"))
    }

    /// Returns the percent-decoded password bytes from the authority component.
    pub fn password(&self) -> Option<Cow<'a, [u8]>> {
        let (_, password) = self.userinfo()?.split_once(':')?;

        if password.as_bytes().contains(&b'%') {
            Some(Cow::Owned(percent_decode_str(password).collect()))
        } else {
            Some(Cow::Borrowed(password.as_bytes()))
        }
    }

    /// Returns the host from the authority component.
    ///
    /// Brackets around an IPv6 address are not included.
    pub fn host(&self) -> Result<Option<&'a str>> {
        let Some(authority) = self.parsed_authority()? else {
            return Ok(None);
        };
        let host = authority.host();
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);

        Ok((!host.is_empty()).then_some(host))
    }

    /// Returns the port from the authority component.
    pub fn port(&self) -> Result<Option<u16>> {
        let Some(authority) = self.parsed_authority()? else {
            return Ok(None);
        };
        authority
            .port_to_u16()
            .map_err(|_| self.invalid_authority())
    }

    /// Validates the host and port syntax of an authority component.
    ///
    /// URLs without an authority component are valid.
    pub fn validate_authority(&self) -> Result<()> {
        self.host()?;
        self.port()?;
        Ok(())
    }

    /// Iterates over the percent-decoded query parameters.
    ///
    /// Query components use form encoding, so `+` decodes to a space.
    pub fn query_pairs(&self) -> impl Iterator<Item = (Cow<'a, str>, Cow<'a, str>)> + '_ {
        self.query()
            .into_iter()
            .flat_map(|query| form_urlencoded::parse(query.as_bytes()))
    }

    /// Returns the URL with an authority password and the values of `password`
    /// query parameters replaced by `***`.
    pub fn redact_password(&self) -> Cow<'a, str> {
        let mut ranges = Vec::new();
        ranges.extend(self.authority_password_range());
        ranges.extend(self.query_password_ranges());

        if ranges.is_empty() {
            return Cow::Borrowed(self.value);
        }

        let mut redacted = String::with_capacity(self.value.len());
        let mut end = 0;
        for range in ranges {
            redacted.push_str(&self.value[end..range.start]);
            redacted.push_str("***");
            end = range.end;
        }
        redacted.push_str(&self.value[end..]);
        Cow::Owned(redacted)
    }

    fn authority_password_range(&self) -> Option<Range<usize>> {
        let authority = self.authority_range()?;
        let authority_value = &self.rest()[authority.clone()];
        let (userinfo, _) = authority_value.rsplit_once('@')?;
        let (username, _) = userinfo.split_once(':')?;

        let password_start = self.scheme_end + 1 + authority.start + username.len() + 1;
        let password_end = self.scheme_end + 1 + authority.start + userinfo.len();
        Some(password_start..password_end)
    }

    fn query_password_ranges(&self) -> Vec<Range<usize>> {
        let Some(query) = self.query_range() else {
            return Vec::new();
        };

        let mut ranges = Vec::new();
        let mut pair_start = query.start;
        for pair in self.value[query].split('&') {
            if let Some((key, _)) = pair.split_once('=')
                && form_urlencoded::parse(key.as_bytes()).any(|(key, _)| key == "password")
            {
                ranges.push(pair_start + key.len() + 1..pair_start + pair.len());
            }
            pair_start += pair.len() + 1;
        }
        ranges
    }

    fn rest(&self) -> &'a str {
        &self.value[self.scheme_end + 1..]
    }

    fn authority_range(&self) -> Option<Range<usize>> {
        let rest = self.rest();
        let authority = rest.strip_prefix("//")?;
        let len = split_before(authority, &['/', '?', '#']).len();
        Some(2..2 + len)
    }

    fn authority(&self) -> Option<&'a str> {
        self.authority_range().map(|range| &self.rest()[range])
    }

    fn userinfo(&self) -> Option<&'a str> {
        self.authority()?
            .rsplit_once('@')
            .map(|(userinfo, _)| userinfo)
    }

    fn parsed_authority(&self) -> Result<Option<fluent_uri::component::IAuthority<'a>>> {
        let Some(range) = self.authority_range() else {
            return Ok(None);
        };
        IriRef::parse(&self.rest()[..range.end])
            .map_err(|_| self.invalid_authority())
            .map(|url| url.authority())
    }

    fn query(&self) -> Option<&'a str> {
        self.query_range().map(|range| &self.value[range])
    }

    fn query_range(&self) -> Option<Range<usize>> {
        let rest = self.rest();
        let query_start = rest.find('?')?;
        if rest[..query_start].contains('#') {
            return None;
        }

        let start = self.scheme_end + 1 + query_start + 1;
        let len = split_before(&rest[query_start + 1..], &['#']).len();
        Some(start..start + len)
    }

    fn invalid_authority(&self) -> Error {
        Error::invalid_connection_url(format!(
            "connection URL has an invalid authority; url={}",
            self.value
        ))
    }
}

fn is_valid_scheme(scheme: &str) -> bool {
    let mut chars = scheme.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
}

fn split_before<'a>(value: &'a str, delimiters: &[char]) -> &'a str {
    value
        .find(delimiters)
        .map_or(value, |index| &value[..index])
}
