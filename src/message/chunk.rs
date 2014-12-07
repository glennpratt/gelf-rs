use std::io;
use std::io::{IoError, IoResult};
use time::{get_time, Timespec};

pub struct Chunk {
    pub id: Vec<u8>,
    pub sequence_number: u8,
    pub sequence_count: u8,
    pub payload: Vec<u8>,
    pub arrival: Option<Timespec>
}

impl Chunk {
    pub fn from_packet(packet: &[u8]) -> IoResult<Chunk> {
        if packet.len() > 12 {
            Ok(Chunk {
                id: packet[2..9].to_vec(),
                sequence_number: packet[10],
                sequence_count: packet[11],
                payload: packet[12..].to_vec(),
                arrival: Some(get_time())
            })
        } else {
            Err(IoError {
                kind: io::InvalidInput,
                desc: "Unsupported GELF: Chunked message must be at least 12 bytes long.",
                detail: None,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn construct_from_packet() {
        let json = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;
        let bar = json[0..1];
    }
}
