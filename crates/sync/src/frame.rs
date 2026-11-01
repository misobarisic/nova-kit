//! Length-prefixed postcard framing shared by the sync and pairing protocols.
//!
//! postcard is a compact, non-self-describing binary serde format. Because it
//! is not self-describing, wire structs must not rely on `#[serde(default)]`:
//! every field is always present. Schema changes therefore require a protocol
//! version bump (the sync ALPN is `/3`).
//!
//! Two framings exist: [`write_frame`]/[`read_frame`] (raw postcard, used by
//! the pairing and removal protocols) and [`write_frame_c`]/[`read_frame_c`]
//! (a codec byte plus optional zstd, used by the sync protocol so its large
//! digests and record batches compress on the wire). Keeping them separate
//! means the pairing/removal wire schema is untouched by compression.

use std::borrow::Cow;
use std::io::{Read, Write};

use anyhow::{Context, Result, bail};
use iroh::endpoint::{ReadExactError, RecvStream, SendStream};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Upper bound for a single frame. Record sets are a personal library plus
/// watch history; pairing frames are tiny.
pub(crate) const MAX_FRAME: usize = 32 * 1024 * 1024;

/// Frame codec byte: the body is postcard as-is.
const CODEC_RAW: u8 = 0;
/// Frame codec byte: the body is a zstd-compressed postcard frame.
/// Codec 1 was DEFLATE in older test builds; reject it rather than attempting
/// an incompatible decode. The ALPN deliberately stays /3 during testing.
const CODEC_ZSTD: u8 = 2;

/// Bodies below this are cheaper to send raw than to pay the zstd framing
/// and per-frame compression overhead.
const COMPRESS_THRESHOLD: usize = 256;

/// Write one `msg` as a 4-byte big-endian length followed by its postcard
/// bytes.
pub(crate) async fn write_frame<T: Serialize>(send: &mut SendStream, msg: &T) -> Result<()> {
    let bytes = postcard::to_allocvec(msg).context("encode frame")?;
    if bytes.len() > MAX_FRAME {
        bail!("frame too large: {} bytes", bytes.len());
    }
    send.write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .context("write frame length")?;
    send.write_all(&bytes).await.context("write frame body")?;
    Ok(())
}

/// Read one frame, or `None` when the peer finished the stream cleanly.
pub(crate) async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<Option<T>> {
    let mut len = [0u8; 4];
    match recv.read_exact(&mut len).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(ReadExactError::FinishedEarly(_)) => bail!("truncated frame header"),
        Err(ReadExactError::ReadError(e)) => return Err(e.into()),
    }
    let n = u32::from_be_bytes(len) as usize;
    if n == 0 || n > MAX_FRAME {
        bail!("bad frame length {n}");
    }
    let mut buf = vec![0u8; n];
    recv.read_exact(&mut buf).await.context("read frame body")?;
    decode(&buf)
}

/// Write one `msg` using the compressible framing: a 4-byte big-endian length,
/// a codec byte, then the body. Bodies at or above [`COMPRESS_THRESHOLD`] are
/// compressed with zstd level 3 when that actually shrinks them; otherwise
/// they stay raw.
///
/// Only the sync protocol uses this, so a compressed frame never reaches a
/// pairing/removal peer.
pub(crate) async fn write_frame_c<T: Serialize>(send: &mut SendStream, msg: &T) -> Result<()> {
    let raw = postcard::to_allocvec(msg).context("encode frame")?;
    let (codec, body) = encode_body(&raw)?;
    // The codec byte counts toward the frame length.
    let total = body.len() + 1;
    if total > MAX_FRAME {
        bail!("frame too large: {total} bytes");
    }
    let mut header = [0u8; 5];
    header[..4].copy_from_slice(&(total as u32).to_be_bytes());
    header[4] = codec;
    send.write_all(&header)
        .await
        .context("write frame header")?;
    send.write_all(&body).await.context("write frame body")?;
    Ok(())
}

/// Read one frame written by [`write_frame_c`], or `None` on clean stream end.
pub(crate) async fn read_frame_c<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<Option<T>> {
    let mut len = [0u8; 4];
    match recv.read_exact(&mut len).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(ReadExactError::FinishedEarly(_)) => bail!("truncated frame header"),
        Err(ReadExactError::ReadError(e)) => return Err(e.into()),
    }
    let n = u32::from_be_bytes(len) as usize;
    if n == 0 || n > MAX_FRAME {
        bail!("bad frame length {n}");
    }
    let mut codec = [0u8; 1];
    recv.read_exact(&mut codec)
        .await
        .context("read frame codec")?;
    let mut body = vec![0u8; n - 1];
    recv.read_exact(&mut body)
        .await
        .context("read frame body")?;
    decode_body(codec[0], &body)
}

fn encode_body(raw: &[u8]) -> Result<(u8, Cow<'_, [u8]>)> {
    if raw.len() > MAX_FRAME {
        bail!("frame too large: {} bytes", raw.len());
    }
    if raw.len() >= COMPRESS_THRESHOLD {
        let packed = compress(raw)?;
        if packed.len() < raw.len() {
            return Ok((CODEC_ZSTD, Cow::Owned(packed)));
        }
    }
    Ok((CODEC_RAW, Cow::Borrowed(raw)))
}

fn decode_body<T: DeserializeOwned>(codec: u8, body: &[u8]) -> Result<Option<T>> {
    match codec {
        CODEC_RAW => decode(body),
        CODEC_ZSTD => decode(&decompress(body)?),
        other => bail!("unknown frame codec {other}"),
    }
}

fn compress(raw: &[u8]) -> Result<Vec<u8>> {
    let mut enc = zstd::stream::Encoder::new(Vec::new(), 3).context("create zstd frame")?;
    enc.include_checksum(true).context("enable zstd checksum")?;
    // Postcard is already buffered, so disclose its exact size. Otherwise the
    // streaming encoder reserves a large default window even for tiny deltas.
    enc.set_pledged_src_size(Some(raw.len() as u64))
        .context("set zstd frame size")?;
    enc.write_all(raw).context("compress frame")?;
    enc.finish().context("finish zstd frame")
}

fn decompress(body: &[u8]) -> Result<Vec<u8>> {
    if let Some(size) = zstd::zstd_safe::get_frame_content_size(body)
        .map_err(|error| anyhow::anyhow!("read zstd header: {error}"))?
        && size > MAX_FRAME as u64
    {
        bail!("decoded frame too large: {size} bytes");
    }
    // Bound the native decoder's window as well as output: a small frame must
    // not request excessive decoder memory or expand beyond the protocol cap.
    let mut decoder = zstd::stream::Decoder::new(body).context("open zstd frame")?;
    decoder.window_log_max(25).context("limit zstd window")?;
    let mut out = Vec::new();
    decoder
        .take(MAX_FRAME as u64 + 1)
        .read_to_end(&mut out)
        .context("decompress frame")?;
    if out.len() > MAX_FRAME {
        bail!("frame too large after decompress: {} bytes", out.len());
    }
    Ok(out)
}

fn decode<T: DeserializeOwned>(raw: &[u8]) -> Result<Option<T>> {
    let (msg, rest) = postcard::take_from_bytes(raw).context("decode frame")?;
    if !rest.is_empty() {
        bail!("trailing bytes after frame");
    }
    Ok(Some(msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zstd_round_trips() {
        let raw = b"abcdef".repeat(1000);
        let packed = compress(&raw).unwrap();
        assert!(packed.len() < raw.len());
        assert_eq!(decompress(&packed).unwrap(), raw);
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&packed).unwrap(),
            Some(raw.len() as u64)
        );
    }

    #[test]
    fn decompress_rejects_oversized_output() {
        // A small compressed body that expands past MAX_FRAME must be
        // rejected rather than allocated.
        let bomb = compress(&vec![0u8; MAX_FRAME + 1]).unwrap();
        assert!(bomb.len() < MAX_FRAME);
        let err = decompress(&bomb).unwrap_err();
        assert!(err.to_string().contains("too large"));
        // Unknown-size frames must also respect the output cap.
        let mut enc = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
        enc.write_all(&vec![0u8; MAX_FRAME + 1]).unwrap();
        let unknown = enc.finish().unwrap();
        assert!(
            decompress(&unknown)
                .unwrap_err()
                .to_string()
                .contains("too large after decompress")
        );
    }

    #[test]
    fn frame_body_round_trips_large_and_small_postcard_messages() {
        for message in ["hello".to_string(), "sync record metadata".repeat(1000)] {
            let raw = postcard::to_allocvec(&message).unwrap();
            let (codec, body) = encode_body(&raw).unwrap();
            assert_eq!(
                codec,
                if raw.len() < COMPRESS_THRESHOLD {
                    CODEC_RAW
                } else {
                    CODEC_ZSTD
                }
            );
            assert_eq!(decode_body::<String>(codec, &body).unwrap(), Some(message));
        }
    }

    #[test]
    fn unprofitable_compression_stays_raw() {
        let mut seed = 123456789_u32;
        let raw = (0..512)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                seed as u8
            })
            .collect::<Vec<_>>();
        let (codec, body) = encode_body(&raw).unwrap();
        assert_eq!(codec, CODEC_RAW);
        assert_eq!(body.as_ref(), raw);
    }

    #[test]
    fn damaged_frames_and_retired_codecs_are_rejected() {
        let packed = compress(&postcard::to_allocvec(&"record".repeat(100)).unwrap()).unwrap();
        assert!(decode_body::<String>(CODEC_ZSTD, &packed[..packed.len() - 1]).is_err());
        let mut damaged = packed.clone();
        *damaged.last_mut().unwrap() ^= 1;
        assert!(decode_body::<String>(CODEC_ZSTD, &damaged).is_err());
        assert!(
            decode_body::<String>(1, &packed)
                .unwrap_err()
                .to_string()
                .contains("unknown frame codec 1")
        );
        assert!(decode_body::<String>(255, &packed).is_err());
        // Streaming encoder emits a window descriptor at byte 5. Advertise a
        // 1 GiB window; rejecting it must precede any large native allocation.
        let mut enc = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
        enc.write_all(b"malicious window").unwrap();
        let mut huge_window = enc.finish().unwrap();
        assert_eq!(huge_window[4] & 0x20, 0);
        huge_window[5] = 0xa0;
        assert!(decompress(&huge_window).is_err());
    }

    #[test]
    fn trailing_postcard_bytes_are_rejected_for_both_codecs() {
        let mut raw = postcard::to_allocvec(&"hello".to_string()).unwrap();
        raw.push(0);
        assert!(decode_body::<String>(CODEC_RAW, &raw).is_err());
        assert!(decode_body::<String>(CODEC_ZSTD, &compress(&raw).unwrap()).is_err());
    }
}
