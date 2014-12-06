use std::io;
use std::io::{IoError, IoResult};
use time::Timespec;

pub struct Chunk {
    id: String,
    sequence_number: u8,
    sequence_count: u8,
    arrival: Timespec,
    payload: Vec<u8>
}

impl Chunk {
    pub fn from_packet(_: &[u8]) -> IoResult<Chunk> {
        Err(IoError {
            kind: io::InvalidInput,
            desc: "Unsupported GELF: Chunked packets are not supported yet.",
            detail: None,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn construct_from_packet() {
        let json = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;
    }
}
