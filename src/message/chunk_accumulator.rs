use std::collections::HashMap;
use std::io::IoResult;
use time::{get_time, Timespec};

use message::Chunk;
use message::unpack_complete;

pub struct ChunkAccumulator {
    map: HashMap<Vec<u8>, ChunkSet>
}

impl ChunkAccumulator {
    pub fn new() -> ChunkAccumulator {
        let map = HashMap::new();
        ChunkAccumulator { map: map }
    }

    pub fn accept(&mut self, chunk: Chunk) -> IoResult<Option<String>> {
        let id = chunk.id.clone();
        if let Some(set) = self.map.get_mut(&id) {
            return set.accept(chunk);
        }
        let mut set = ChunkSet::new(&chunk);
        match set.accept(chunk) {
            Ok(None)   => {
                self.map.insert(id, set);
                Ok(None)
            }
            Ok(string) => {
                self.map.remove(&id);
                Ok(string)
            },
            Err(e)     => Err(e)
        }
    }
}

struct ChunkSet {
    chunks: Vec<Option<Chunk>>,
    rcv_count: uint,
    first_arrival: Timespec
}

impl ChunkSet {
    pub fn new(chunk: &Chunk) -> ChunkSet {
        let mut chunks = vec![];
        chunks.grow(chunk.sequence_count.to_uint().unwrap(), None);
        let number = chunk.sequence_number.to_uint().unwrap();
        let arrival = chunk.arrival;
        ChunkSet { chunks: chunks, first_arrival: arrival, rcv_count: 0 }
    }

    pub fn accept(&mut self, chunk: Chunk) -> IoResult<Option<String>> {
        let number = chunk.sequence_number.to_uint().unwrap();
        let index = chunk.sequence_count.to_uint().unwrap() - 1;
        // if  index != self.chunks.len() || index >= number {
        //     panic!("invalid - this shouldn't panic tho :)");
        // }
        match self.chunks[index] {
            None => {
                self.chunks[index] = Some(chunk);
                self.rcv_count += 1;
                self.complete_or_none()
            },
            Some(_) => Ok(None) // @todo duplicate packet, error or meh? Java overwrites (maybe)?
        }
    }

    fn complete_or_none(&self) -> IoResult<Option<String>> {
        if self.rcv_count == self.chunks.len() {
            let complete_message = self.chunks[0].clone().unwrap().payload;
            // let complete_message = self.chunks[0].unwrap().payload.clone();
            // Delete or allow cleanup thread to do that?
            // Generate complete message.
            Ok(Some(try!(unpack_complete(complete_message.as_slice()))))
            // Ok(None)
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;
    use message::Chunk;

    #[test]
    fn single_chunk() {
        // Who knows if this should even be supported?
        let json = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;

        let chunks = chunker(json, 100);
        let packet = chunks[0].as_slice();

        let chunk = Chunk::from_packet(packet).unwrap();
        let mut acc = ChunkAccumulator::new();
        let result = acc.accept(chunk).unwrap().unwrap();
        assert_eq!(json, result.as_slice());
    }

    #[test]
    fn two_chunks() {
        // Who knows if this should even be supported?
        let json = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;

        let chunks = chunker(json, 22);
        let packet = chunks[0].as_slice();

        let chunk = Chunk::from_packet(packet).unwrap();
        let mut acc = ChunkAccumulator::new();
        let result = acc.accept(chunk).unwrap().unwrap();
        assert_eq!(json, result.as_slice());
    }

    fn chunker(message: &str, max_length: uint) -> Vec<Vec<u8>> {

        let id = "amessage"; // @todo is this hex in other impls?

        let length = if message.len() > max_length {
            max_length
        } else {
            message.len()
        };

        let mut count = message.len() / length;
        let remainder = message.len() % length;
        if remainder != 0 {
            count += 1;
        }
        // Limit to max (255 for u8, GELF is lower, but meh).
        // Panics otherwise, should return Result(Err).
        let sequence_count = count.to_u8().unwrap();
        // println!("{}", message.as_bytes().slice_to(length));
        let mut chunks = vec![];
        for x in range(0, sequence_count) {
            println!("x     : {}", x);
            let sequence_number = (x + 1);

            let start = length * x.to_uint().unwrap();
            let end = length * (x + 1).to_uint().unwrap();
            println!("start: {}", start);
            println!("end  : {}", end);
            let part = if end <= length {
                message.as_bytes()[start..end]
            } else {
                message.as_bytes()[start..]
            };

            let mut bytes: Vec<u8> = vec![0x1e, 0x0f];
            bytes.push_all(id.as_bytes());
            bytes.push(sequence_number);
            bytes.push(sequence_count);
            bytes.push_all(part);
            println!("part: {}", part);
            println!("part len: {}", part.len());
            println!("bytes: {}", bytes);
            println!("bytes len: {}", bytes.len());
            chunks.push(bytes);
        }
        // for x in range()
        println!("chunks {}", chunks);
        chunks
    }
}
