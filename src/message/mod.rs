use std::io::prelude::*;
use std::io;
use flate2::read::{GzDecoder, ZlibDecoder};
use std::str;

pub use self::chunk::Chunk;
pub use self::Payload::*;

pub mod chunk;

pub enum Payload {
    Complete(String),
    Partial(Chunk)
}

pub fn unpack(packet: &[u8]) -> io::Result<Payload> {
    match packet {
        [0x1e, 0x0f, ..] => Ok(Partial(try!(Chunk::from_packet(packet)))),
        _                => Ok(Complete(try!(unpack_complete(packet))))
    }
}

pub fn unpack_complete(packet: &[u8]) -> io::Result<String> {
    match packet {
        [0x1f, 0x8b, ..]            => unpack_gzip(packet),
        [0x78, y, ..] if is_zlib(y) => unpack_zlib(packet),
        [_, _, ..]                  => unpack_uncompressed(packet),
        _                           => Err(io::Error::new(
                                             io::ErrorKind::InvalidInput,
                                             "GELF: Packet too short, less than 2 bytes."))
    }
}

#[inline]
fn is_zlib(second_byte: u8) -> bool {
    (256 * 0x78 + second_byte as u16) % 31 == 0
}

fn unpack_gzip(packet: &[u8]) -> io::Result<String> {
    let mut string = String::new();
    let mut decoder = try!(GzDecoder::new(packet));
    try!(decoder.read_to_string(&mut string));
    Ok(string)
}

fn unpack_zlib(packet: &[u8]) -> io::Result<String> {
    let mut string = String::new();
    try!(ZlibDecoder::new(packet).read_to_string(&mut string));
    Ok(string)
}

fn unpack_uncompressed(packet: &[u8]) -> io::Result<String> {
    match str::from_utf8(packet) {
        Ok(payload) => Ok(payload.to_string()),
        Err(e)      => Err(io::Error::new(io::ErrorKind::InvalidInput,
                                          "GELF: Unknown, non-UTF8 payload."))
    }
}

#[cfg(test)]
mod test {
    extern crate test;

    use self::test::Bencher;
    use super::*;
    use std::io::prelude::*;
    use flate2::Compression;
    use flate2::write::{GzEncoder, ZlibEncoder};

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
        let mut e = GzEncoder::new(Vec::new(), Compression::Default);
        e.write(json.as_bytes());
        let byte_vec = e.finish().unwrap();
        let packet = byte_vec.as_slice();

        match unpack(packet).unwrap() {
            Partial(_) => assert!(false, "Expected 'Complete' result."),
            Complete(s) => assert_eq!(json, s.as_slice())
        }
    }

    #[test]
    fn unpack_with_zlib() {
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;
        let mut e = ZlibEncoder::new(Vec::new(), Compression::Default);
        e.write(json.as_bytes());
        let byte_vec = e.finish().unwrap();
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
        let mut e = ZlibEncoder::new(Vec::new(), Compression::Default);
        e.write(json.as_bytes());
        let byte_vec = e.finish().unwrap();
        let packet = byte_vec.as_slice();

        b.iter(|| unpack(packet));
    }

    #[bench]
    fn bench_gzip(b: &mut Bencher) {
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;
        let mut e = GzEncoder::new(Vec::new(), Compression::Default);
        e.write(json.as_bytes());
        let byte_vec = e.finish().unwrap();
        let packet = byte_vec.as_slice();

        b.iter(|| unpack(packet));
    }
}

#[cfg(test)]
mod test_udp_receiver {
    use super::*;
    use std::net::{UdpSocket, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
    use std::sync::mpsc::channel;
    use std::thread;

    #[test]
    fn udp_receiver_smoke_test() {
        // TODO: Figure out how to import std::net::test::next_test_ip4() in the new io.
        let server_ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6575));
        let client_ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6576));
        let (tx1, rx1) = channel();
        let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;

        thread::spawn(move|| {
            match UdpSocket::bind(client_ip) {
                Ok(ref mut client) => {
                    rx1.recv().unwrap(); // Wait for signal main thread is listening.
                    client.send_to(json.as_bytes(), server_ip).unwrap();
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
                        let packet = &buf.as_slice()[..n_read];
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
    }
}
