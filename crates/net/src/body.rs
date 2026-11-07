use std::io::{self, Read};

#[derive(Debug)]
pub enum ReadError {
    /// The source holds more than the cap allows.
    TooLarge,
    Io(io::Error),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::TooLarge => f.write_str("body exceeds its byte limit"),
            ReadError::Io(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ReadError {}
impl From<io::Error> for ReadError {
    fn from(error: io::Error) -> Self {
        ReadError::Io(error)
    }
}

/// Read all of `reader`, failing if it yields more than `limit` bytes. At most
/// `limit + 1` bytes are ever buffered, so an endless or oversized source
/// cannot exhaust memory.
pub fn read_capped(reader: impl Read, limit: u64) -> Result<Vec<u8>, ReadError> {
    let mut body = Vec::new();
    reader
        .take(limit.saturating_add(1))
        .read_to_end(&mut body)?;
    if body.len() as u64 > limit {
        return Err(ReadError::TooLarge);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_body_at_the_limit_is_returned_and_one_more_byte_is_refused() {
        assert_eq!(read_capped(&[7u8; 10][..], 10).unwrap(), vec![7; 10]);
        assert!(matches!(
            read_capped(&[7u8; 11][..], 10),
            Err(ReadError::TooLarge)
        ));
        assert!(read_capped(&[][..], 0).unwrap().is_empty());
    }

    #[test]
    fn an_endless_source_stops_at_the_cap() {
        assert!(matches!(
            read_capped(io::repeat(1), 1 << 16),
            Err(ReadError::TooLarge)
        ));
    }

    #[test]
    fn read_errors_are_not_reported_as_oversize() {
        struct Failing;
        impl Read for Failing {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("reset"))
            }
        }
        assert!(matches!(read_capped(Failing, 10), Err(ReadError::Io(_))));
    }
}
