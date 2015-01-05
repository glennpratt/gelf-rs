use flate2::FlateReader;
use std::str;
// use std::str::Utf8Error;
use std::io;
use std::io::{BufReader, IoResult, IoError};

pub use self::chunk::Chunk;
pub use self::chunk_accumulator::ChunkAccumulator;
pub use self::Payload::*;

pub mod chunk;
pub mod chunk_accumulator;

pub enum Payload {
    Complete(String),
    Partial(Chunk)
}

pub fn unpack(packet: &[u8]) -> IoResult<Payload> {
    match packet {
        [0x1e, 0x0f, ..] => Ok(Partial(try!(Chunk::from_packet(packet)))),
        _                => Ok(Complete(try!(unpack_complete(packet))))
    }
}

pub fn unpack_complete(packet: &[u8]) -> IoResult<String> {
    match packet {
        [0x1f, 0x8b, ..]            => unpack_gzip(packet),
        [0x78, y, ..] if is_zlib(y) => unpack_zlib(packet),
        [_, _, ..]                  => unpack_uncompressed(packet),
        _                           => Err(IoError {
            kind: io::InvalidInput,
            desc: "Unsupported GELF: Packet too short, less than 2 bytes.",
            detail: None,
        })
    }
}

fn unpack_gzip(packet: &[u8]) -> IoResult<String> {
    let mut reader = BufReader::new(packet).gz_decode();
    let bytes = try!(reader.read_to_end());
    unpack_uncompressed(bytes.as_slice().clone())
}

fn unpack_zlib(packet: &[u8]) -> IoResult<String> {
    let mut reader = BufReader::new(packet).zlib_decode();
    let bytes = try!(reader.read_to_end());
    unpack_uncompressed(bytes.as_slice().clone())
}

fn unpack_uncompressed(packet: &[u8]) -> IoResult<String> {
    match str::from_utf8(packet) {
        Ok(payload) => Ok(payload.to_string()),
        Err(_)      => Err(IoError {
            kind: io::InvalidInput,
            desc: "Unsupported GELF: Unknown, non-UTF8 payload.",
            detail: None,
        })
    }
}

#[inline(always)]
fn is_zlib(second_byte: u8) -> bool {
    (256 * 0x78 + second_byte as u16) % 31 == 0
}

#[cfg(test)]
mod test {
    extern crate test;

    use self::test::Bencher;
    use super::*;
    use flate2::{FlateReader, CompressionLevel};
    use std::io::{BufReader};

    #[test]
    fn unpack_with_uncompressed() {
        let json = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;
        let packet = json.clone().as_bytes();

        match unpack(packet).unwrap() {
            Partial(_) => assert!(false, "Expected 'Complete' result."),
            Complete(s) => assert_eq!(json, s.as_slice())
        }
    }

    #[test]
    fn unpack_with_gzip() {
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;
        let rdr = BufReader::new(json.as_bytes());
        let byte_vec = rdr.gz_encode(CompressionLevel::Default).read_to_end().unwrap();
        let packet = byte_vec.as_slice();

        match unpack(packet).unwrap() {
            Partial(_) => assert!(false, "Expected 'Complete' result."),
            Complete(s) => assert_eq!(json, s.as_slice())
        }
    }

    #[test]
    fn unpack_with_zlib() {
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;
        let rdr = BufReader::new(json.as_bytes());
        let byte_vec = rdr.zlib_encode(CompressionLevel::Default).read_to_end().unwrap();
        let packet = byte_vec.as_slice();

        match unpack(packet).unwrap() {
            Partial(_) => assert!(false, "Expected 'Complete' result."),
            Complete(s) => assert_eq!(json, s.as_slice())
        }
    }

    #[test]
    fn unpack_errors_with_no_bytes() {
        let packet: &[u8] = &[];

        assert!(unpack(packet).is_err());
    }

    #[test]
    fn unpack_errors_with_one_byte() {
        let packet: &[u8] = &[0x0f];

        assert!(unpack(packet).is_err());
    }

    #[bench]
    fn bench_uncompressed(b: &mut Bencher) {
        let json = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;
        let packet = json.clone().as_bytes();

        b.iter(|| unpack(packet));
    }

    #[bench]
    fn bench_zlib(b: &mut Bencher) {
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;
        let rdr = BufReader::new(json.as_bytes());
        let byte_vec = rdr.zlib_encode(CompressionLevel::Default).read_to_end().unwrap();
        let packet = byte_vec.as_slice();

        b.iter(|| unpack(packet));
    }

    #[bench]
    fn bench_gzip(b: &mut Bencher) {
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;
        let rdr = BufReader::new(json.as_bytes());
        let byte_vec = rdr.gz_encode(CompressionLevel::Default).read_to_end().unwrap();
        let packet = byte_vec.as_slice();

        b.iter(|| unpack(packet));
    }
}

#[cfg(test)]
mod test_udp_receiver {
    use super::*;
    use std::io::net::udp::*;
    use std::io::test::*;
    use std::sync::mpsc::channel;
    use std::thread::Thread;

    #[test]
    fn udp_receiver_smoke_test() {
        let server_ip = next_test_ip4();
        let client_ip = next_test_ip4();
        let (tx1, rx1) = channel();
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;

        let thread = Thread::spawn(move|| {
            match UdpSocket::bind(client_ip) {
                Ok(ref mut client) => {
                    rx1.recv().unwrap(); // Wait for signal main thread is listening.
                    client.send_to(json.as_bytes(), server_ip).unwrap()
                }
                Err(..) => panic!()
            }
        });

        match UdpSocket::bind(server_ip) {
            Ok(ref mut server) => {
                tx1.send(()).unwrap();

                // From gelfclient... CHUNK_MAGIC_BYTES(2) + messageId(8) + sequenceNumber(1) + sequenceCount(1) + MAX_CHUNK_SIZE(1420)
                let mut buf = [0; 1432];
                match server.recv_from(&mut buf) {
                    Ok((n_read, _)) => {
                        let packet = buf.as_slice().slice_to(n_read);
                        match unpack(packet).unwrap() {
                            Partial(_) => assert!(false, "Expected 'Complete' result."),
                            Complete(s) => assert_eq!(json, s.as_slice())
                        }
                    }
                    Err(..) => panic!()
                }
            }
            Err(..) => panic!()
        }
        // Join thread with OK result or panic.
        thread.join().ok().unwrap();
    }
}
