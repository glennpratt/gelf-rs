use std::old_io;
use std::old_io::{IoError, IoResult};
use time::{get_time, Timespec};

#[derive(Clone, Show)]
pub struct Chunk {
    pub id: Vec<u8>,
    pub sequence_number: u8,
    pub sequence_count: u8,
    pub payload: Vec<u8>,
    pub arrival: Timespec
}

impl Chunk {
    pub fn from_packet(packet: &[u8]) -> IoResult<Chunk> {
        if packet.len() > 12 {
            Ok(Chunk {
                id: packet[2..10].to_vec(),
                sequence_number: packet[10],
                sequence_count: packet[11],
                payload: packet[12..].to_vec(),
                arrival: get_time()
            })
        } else {
            Err(IoError {
                kind: old_io::InvalidInput,
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
        let id = "amessage"; // @todo is this hex in other impls?
        let sequence_number = 1;
        let sequence_count =  1;

        let mut bytes: Vec<u8> = vec![0x1e, 0x0f];
        bytes.push_all(id.as_bytes());
        bytes.push(sequence_number);
        bytes.push(sequence_count);
        bytes.push_all(json.as_bytes());
        let packet = bytes.as_slice();

        let chunk = Chunk::from_packet(packet).unwrap();

        assert_eq!(id.as_bytes(), chunk.id.as_slice());
        assert_eq!(sequence_number, chunk.sequence_number);
        assert_eq!(sequence_count, chunk.sequence_count);
        assert_eq!(json.as_bytes(), chunk.payload.as_slice());
        println!("{:?}", chunk.arrival);
    }
}
