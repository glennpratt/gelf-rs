extern crate flate2;

use std::io;
use std::io::{BufReader, IoResult, IoError};
use flate2::{FlateReader, CompressionLevel};

pub fn unpack(packet: &[u8]) -> IoResult<&str> {
    let magic_bytes = packet.slice_to(2);
    let chunk_magic: &[u8] = &[0x1e, 0x0f];
    let gzip_magic: &[u8] = &[0x1f, 0x8b];
    let zlib_magic: &[u8] = &[0x78, 0x01]; // @todo - Match all compression levels.

    if gzip_magic == magic_bytes {
        unpack_gzip(packet)
    } else if zlib_magic == magic_bytes {
        unpack_zlib(packet)
    } else if chunk_magic == magic_bytes {
        unpack_chunk(packet)
    } else {
        unpack_uncompressed(packet)
    }
}

fn unpack_chunk(_: &[u8]) -> IoResult<&str> {
    Err(IoError {
        kind: io::InvalidInput,
        desc: "Unsupported GELF: Chunked packets are not supported yet.",
        detail: None,
    })
}

fn unpack_gzip(packet: &[u8]) -> IoResult<&str> {
    let reader = BufReader::new(packet);
    let bytes = try!(reader.gz_decode().read_to_end());
    // @todo - why do I need clone here?
    unpack_uncompressed(bytes.as_slice().clone())
}

fn unpack_zlib(packet: &[u8]) -> IoResult<&str> {
    let reader = BufReader::new(packet);
    let bytes = try!(reader.zlib_decode().read_to_end());
    // @todo - why do I need clone here?
    unpack_uncompressed(bytes.as_slice().clone())
}

fn unpack_uncompressed(packet: &[u8]) -> IoResult<&str> {
    match std::str::from_utf8(packet) {
        Some(payload) => Ok(payload),
        None => Err(IoError {
            kind: io::InvalidInput,
            desc: "Unsupported GELF: Unknown, non-UTF8 payload.",
            detail: None,
        })
    }
}

#[test]
fn test_unpack_with_uncompressed() {
    let json = r#"{\"message\":\"foo\",\"host\":\"bar\",\"_lol_utf8\":\"\u00FC\"}"#;
    let packet = json.clone().as_bytes();
    
    assert_eq!(json, unpack(packet).unwrap());
}

#[test]
fn test_unpack_with_gzip() {
    let json = r#"{\"message\":\"foo\",\"host\":\"bar\",\"_lol_utf8\":\"\u00FC\"}"#;
    let rdr = BufReader::new(json.as_bytes());
    let byte_vec = rdr.gz_encode(CompressionLevel::Default).read_to_end().unwrap();

    assert_eq!(json, unpack(byte_vec.as_slice()).unwrap());
}

#[test]
fn test_unpack_with_zlib() {
    let json = r#"{\"message\":\"foo\",\"host\":\"bar\",\"_lol_utf8\":\"\u00FC\"}"#;
    let rdr = BufReader::new(json.as_bytes());
    let byte_vec = rdr.zlib_encode(CompressionLevel::Default).read_to_end().unwrap();

    assert_eq!(json, unpack(byte_vec.as_slice()).unwrap());
}
