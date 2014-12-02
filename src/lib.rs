#![feature(globs)]

extern crate flate2;

use flate2::FlateReader;
use std::io;
use std::io::{BufReader, IoResult, IoError};

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
    let mut reader = BufReader::new(packet).gz_decode();
    let bytes = try!(reader.read_to_end());
    // @todo - why do I need clone here?
    unpack_uncompressed(bytes.as_slice().clone())
}

fn unpack_zlib(packet: &[u8]) -> IoResult<&str> {
    let mut reader = BufReader::new(packet).zlib_decode();
    let bytes = try!(reader.read_to_end());
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

#[cfg(test)]
mod test {
    use unpack;
    use flate2::{FlateReader, CompressionLevel};
    use std::io::{BufReader};

    #[test]
    fn unpack_with_uncompressed() {
        let json = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;
        let packet = json.clone().as_bytes();

        assert_eq!(json, unpack(packet).unwrap());
    }

    #[test]
    fn unpack_with_gzip() {
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;
        let rdr = BufReader::new(json.as_bytes());
        let byte_vec = rdr.gz_encode(CompressionLevel::Default).read_to_end().unwrap();

        assert_eq!(json, unpack(byte_vec.as_slice()).unwrap());
    }

    #[test]
    fn unpack_with_zlib() {
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;
        let rdr = BufReader::new(json.as_bytes());
        let byte_vec = rdr.zlib_encode(CompressionLevel::Default).read_to_end().unwrap();

        assert_eq!(json, unpack(byte_vec.as_slice()).unwrap());
    }
}

#[cfg(test)]
mod test_udp_receiver {
    use unpack;
    use std::io::net::udp::*;
    use std::prelude::*;
    use std::io::test::*;

    #[test]
    fn udp_receiver_smoke_test() {
        let server_ip = next_test_ip4();
        let client_ip = next_test_ip4();
        let (tx1, rx1) = channel();
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;

        spawn(proc() {
            match UdpSocket::bind(client_ip) {
                Ok(ref mut client) => {
                    rx1.recv(); // Wait for signal main thread is listening.
                    client.send_to(json.as_bytes(), server_ip).unwrap()
                }
                Err(..) => panic!()
            }
        });

        match UdpSocket::bind(server_ip) {
            Ok(ref mut server) => {
                tx1.send(());

                // From gelfclient... CHUNK_MAGIC_BYTES(2) + messageId(8) + sequenceNumber(1) + sequenceCount(1) + MAX_CHUNK_SIZE(1420)
                let mut buf = [0, ..1432];
                match server.recv_from(&mut buf) {
                    Ok((n_read, src)) => {
                        assert_eq!(json, unpack(buf.as_slice().slice_to(n_read)).unwrap());
                        assert_eq!(src, client_ip);
                    }
                    Err(..) => panic!()
                }
            }
            Err(..) => panic!()
        }
    }
}
