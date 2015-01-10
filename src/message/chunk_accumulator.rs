use std::collections::HashMap;
use std::io::{IoResult, Timer};
use std::iter::repeat;
use std::ops::Drop;
use std::sync::{Arc,Mutex};
use std::sync::mpsc::{channel, Sender};
use std::thread::{JoinGuard, Thread};
use std::time::Duration;

use time::{get_time, Timespec};

use message::Chunk;
use message::unpack_complete;

use self::ChunkAccumulatorSignal::{EvictionEntry, Quit};

enum ChunkAccumulatorSignal {
    EvictionEntry((Vec<u8>, Timespec)),
    Quit
}

pub struct ChunkAccumulator {
    map: Arc<Mutex<HashMap<Vec<u8>, ChunkSet>>>,
    tx: Sender<ChunkAccumulatorSignal>,
    reaper: Option<JoinGuard<()>>
}

impl ChunkAccumulator {
    pub fn new() -> ChunkAccumulator {
        let (tx, rx) = channel();
        let mutex_map = Arc::new(Mutex::new(HashMap::new()));
        let reaper_mutex_map = mutex_map.clone();

        let thread = Thread::spawn(move|| {
            let mut eviction_lifo: Vec<(Vec<u8>, Timespec)> = vec![];
            let mut timer = Timer::new().unwrap();
            let validity = Duration::seconds(5);

            loop {
                let timeout = if eviction_lifo.len() > 0 {
                    let (_, arrival) = eviction_lifo[0];
                    let eviction_time = arrival + validity;
                    timer.oneshot(eviction_time - get_time())
                } else {
                    // I really want a null receiver here, but aliasing rules
                    // make that hard.
                    timer.oneshot(Duration::days(1000))
                };
                select! (
                    signal = rx.recv() => {
                        match signal.unwrap() {
                            EvictionEntry(entry) => { eviction_lifo.push(entry); },
                            Quit => { break; }
                        }
                    },
                    _ = timeout.recv() => {
                        let (id, _) = eviction_lifo.remove(0);
                        let mut map = reaper_mutex_map.lock().unwrap();
                        (*map).remove(&id);
                        drop(map);
                    }
                )
            }
        });
        let acc = ChunkAccumulator { map: mutex_map, tx: tx, reaper: Some(thread) };
        acc
    }

    pub fn accept(&mut self, chunk: Chunk) -> IoResult<Option<String>> {
        let id = chunk.id.clone();
        let mut map = self.map.lock().unwrap();
        if let Some(set) = (*map).get_mut(&id) {
            return set.accept(chunk);
        }
        let mut set = ChunkSet::new(&chunk);
        match set.accept(chunk) {
            Ok(None)   => {
                self.tx.send(EvictionEntry((id.clone(), set.first_arrival.clone())));
                (*map).insert(id, set);
                Ok(None)
            }
            Ok(string) => Ok(string),
            Err(e)     => Err(e)
        }
    }
}

impl Drop for ChunkAccumulator {
    fn drop(&mut self) {
        self.tx.send(Quit);
        // Replace reaper with None in the struct, confirm it's a thread, join
        // the thread and confirm it's result was Ok or panic.
        self.reaper.take().unwrap().join().ok().unwrap();
    }
}

struct ChunkSet {
    chunks: Vec<Option<Chunk>>,
    rcv_count: usize,
    pub first_arrival: Timespec
}

impl ChunkSet {
    pub fn new(chunk: &Chunk) -> ChunkSet {
        let size = chunk.sequence_count as usize;
        let chunks = repeat(None).take(size).collect();
        let arrival = chunk.arrival;
        ChunkSet { chunks: chunks, first_arrival: arrival, rcv_count: 0 }
    }

    pub fn accept(&mut self, chunk: Chunk) -> IoResult<Option<String>> {
        let number = chunk.sequence_number as usize - 1;
        // let index = chunk.sequence_count.to_usize().unwrap() - 1;
        // if  index != self.chunks.len() || index >= number {
        //     panic!("invalid - this shouldn't panic tho :)");
        // }
        match self.chunks[number] {
            None => {
                self.chunks[number] = Some(chunk);
                self.rcv_count += 1;
                self.complete_or_none()
            },
            Some(_) => Ok(None) // @todo duplicate packet, error or meh? Java overwrites (maybe)?
        }
    }

    // @todo This should be done outside any lock! No reason to decompress under
    // a lock, so return a complete ChunkSet removed from the HashMap for the 
    // worker to decompress on it's own time.
    fn complete_or_none(&self) -> IoResult<Option<String>> {
        if self.rcv_count == self.chunks.len() {
            // Just return ChunkSet, move this to ChunkSet.
            let mut complete_message = vec![];
            for chunk in self.chunks.iter() {
                complete_message.push_all(chunk.clone().unwrap().payload.as_slice());
            }
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
    use std::rand::{OsRng, Rng};
    use std::io::timer::sleep;
    use std::time::Duration;
    use time::get_time;
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
        let json = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;

        let chunks = chunker(json, 22);
        let packet1 = chunks[0].as_slice();
        let packet2 = chunks[1].as_slice();

        let chunk1 = Chunk::from_packet(packet1).unwrap();
        let chunk2 = Chunk::from_packet(packet2).unwrap();
        let mut acc = ChunkAccumulator::new();
        acc.accept(chunk1).unwrap();
        let result = acc.accept(chunk2).unwrap().unwrap();
        assert_eq!(json, result.as_slice());
    }

    #[test]
    fn reaper() {
        let json = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;

        let chunks = chunker(json, 22);
        let packet1 = chunks[0].as_slice();
        let packet2 = chunks[1].as_slice();

        let mut chunk1 = Chunk::from_packet(packet1).unwrap();
        // Backdate chunk1 arrival so it's already expired.
        chunk1.arrival = get_time() - Duration::seconds(6);
        let chunk2 = Chunk::from_packet(packet2).unwrap();
        let mut acc = ChunkAccumulator::new();
        acc.accept(chunk1).unwrap();
        sleep(Duration::milliseconds(1)); // Allow reaper thread to run.
        let option = acc.accept(chunk2).unwrap();
        // The first packet expired, so the second doesn't complete anything.
        assert_eq!(None, option);
    }

    #[test]
    fn two_chunked_messages() {
        let json_a = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;

        let chunks_a = chunker(json_a, 22);

        let json_b = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;

        let chunks_b = chunker(json_b, 22);
        let chunk_b_1 = Chunk::from_packet(chunks_b[0].as_slice()).unwrap();
        let chunk_b_2 = Chunk::from_packet(chunks_b[1].as_slice()).unwrap();

        let chunk_a_1 = Chunk::from_packet(chunks_a[0].as_slice()).unwrap();
        let chunk_a_2 = Chunk::from_packet(chunks_a[1].as_slice()).unwrap();
        let mut acc = ChunkAccumulator::new();
        acc.accept(chunk_a_1).unwrap();
        acc.accept(chunk_b_1).unwrap();
        let result_a = acc.accept(chunk_a_2).unwrap().unwrap();
        assert_eq!(json_a, result_a.as_slice());
        let result_b = acc.accept(chunk_b_2).unwrap().unwrap();
        assert_eq!(json_b, result_b.as_slice());
    }

    fn chunker(message: &str, max_length: usize) -> Vec<Vec<u8>> {
        // Test only id.
        let mut id = [0u8; 8];
        let mut rng = OsRng::new().unwrap();
        rng.fill_bytes(&mut id);

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
        // @todo this truncates...
        let sequence_count = count as u8;
        let mut chunks = vec![];
        for x in range(0, sequence_count) {
            let sequence_number = x + 1;

            let start = length * x as usize;
            let end = length * (x as usize + 1);
            let part = if end <= length {
                &message.as_bytes()[start..end]
            } else {
                &message.as_bytes()[start..]
            };

            let mut bytes: Vec<u8> = vec![0x1e, 0x0f];
            bytes.push_all(id.as_slice());
            bytes.push(sequence_number);
            bytes.push(sequence_count);
            bytes.push_all(part);
            chunks.push(bytes);
        }
        chunks
    }
}
