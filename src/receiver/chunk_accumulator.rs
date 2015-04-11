use std::collections::HashMap;
use std::io::prelude::*;
use std::io;
use std::iter::repeat;
use std::old_io::timer::Timer;
use std::ops::Drop;
use std::sync::{Arc,Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::thread::{JoinGuard, Thread};
use std::time::Duration;

use time;
use time::{get_time, Timespec};

use message::Chunk;
use message::unpack_complete;

enum Signal {
    EvictionEntry((Vec<u8>, Duration)),
    Quit
}

pub struct ChunkAccumulator {
    map: Arc<Mutex<HashMap<Vec<u8>, ChunkSet>>>,
    reaper_tx: Sender<Signal>,
    reaper: Option<JoinGuard<'static ()>>
}

impl ChunkAccumulator {
    pub fn new() -> ChunkAccumulator {
        let (tx, rx) = channel();
        let map_mutex = Arc::new(Mutex::new(HashMap::new()));
        let reaper_map_mutex = map_mutex.clone();

        // Start a reaper thread to evict expired chunks from the HashMap. This
        // thread should have the same lifetime as the struct.
        let thread = thread::scoped(move|| {
            ChunkAccumulator::reaper(reaper_map_mutex, rx);
        });

        ChunkAccumulator {
            map: map_mutex,
            reaper_tx: tx,
            reaper: Some(thread)
        }
    }

    pub fn accept(&mut self, chunk: Chunk) -> io::Result<Option<ChunkSet>> {
        let id = chunk.id.clone();
        let mut map = self.map.lock().unwrap();

        if (*map).contains_key(&id) {
            // This is a bit convoluted because of lexical borrows. The
            // get_mut().unwrap() should never panic because we've already run
            // contains_key() under a lock(). With non-lexical borrows, this
            // can be a single match or if-let.
            let result = (*map).get_mut(&id).unwrap().accept(chunk);
            return match result {
                ChunkSetState::Complete => Ok((*map).remove(&id)),
                _                       => Ok(None),
            }
        }

        let mut new_set = ChunkSet::new(&chunk);
        match new_set.accept(chunk) {
            ChunkSetState::Complete  => Ok(Some(new_set)),
            ChunkSetState::Partial   => {
                let eviction_entry = Signal::EvictionEntry((
                    id.clone(),
                    new_set.expires_in()
                ));
                self.reaper_tx.send(eviction_entry).ok().expect("Communication with Reaper thread failed.");
                (*map).insert(id, new_set);
                Ok(None)
            }
        }
    }

    fn reaper(map_mutex: Arc<Mutex<HashMap<Vec<u8>, ChunkSet>>>, rx: Receiver<Signal>) {
        let mut eviction_fifo: Vec<(Vec<u8>, Duration)> = vec![];
        let mut timer = Timer::new().unwrap();
        let validity = Duration::seconds(5);
        // Get a receiver that will never recv() for when we don't have a
        // timeout.
        let (_never_tx, never_rx) = channel::<()>();
        // Move the never_rx into an Option so it isn't aliased as timeout
        // when used.
        // @todo this seems ugly, find a better way. Two different select!s?
        let mut never_rx_opt = Some(never_rx);

        loop {
            let timeout = if eviction_fifo.len() > 0 {
                let (_, expires_in) = eviction_fifo[0];
                timer.oneshot(expires_in)
            } else {
                never_rx_opt.take().expect("Reaper null receiver was None. This should never happen")
            };
            select!(
                msg = rx.recv() => match msg.unwrap() {
                    Signal::EvictionEntry(e) => eviction_fifo.push(e),
                    Signal::Quit             => break,
                },
                _ = timeout.recv() => {
                    let (id, _) = eviction_fifo.remove(0);
                    let mut map = map_mutex.lock().unwrap();
                    (*map).remove(&id);
                }
            );
            // Move never_rx back if we used it.
            if never_rx_opt.is_none() {
                never_rx_opt = Some(timeout);
            }
        }
    }
}

impl Drop for ChunkAccumulator {
    fn drop(&mut self) {
        let _ = self.reaper_tx.send(Signal::Quit);
        if let Some(thread) = self.reaper.take() {
            thread.join();
        }
    }
}

enum ChunkSetState {
    Partial,
    Complete
}

#[derive(Debug)]
pub struct ChunkSet {
    chunks: Vec<Option<Chunk>>,
    rcv_count: usize,
    first_arrival: Timespec
}

impl ChunkSet {
    fn new(chunk: &Chunk) -> ChunkSet {
        let size = chunk.sequence_count as usize;
        let chunks = repeat(None).take(size).collect();
        let arrival = chunk.arrival;

        ChunkSet {
            chunks: chunks,
            first_arrival: arrival,
            rcv_count: 0
        }
    }

    fn accept(&mut self, chunk: Chunk) -> ChunkSetState {
        let number = chunk.sequence_number as usize - 1;
        match self.chunks[number] {
            None => {
                self.chunks[number] = Some(chunk);
                self.rcv_count += 1;
                if self.rcv_count == self.chunks.len() {
                    ChunkSetState::Complete
                } else {
                    ChunkSetState::Partial
                }
            },
            // @todo duplicate packet, error or meh? Java overwrites (maybe)?
            Some(_) => ChunkSetState::Partial
        }
    }

    fn expires_in(&self) -> Duration {
        let validity = time::Duration::seconds(5);
        let eviction_time = self.first_arrival + validity;
        Duration::seconds((eviction_time - get_time()).num_seconds())
    }

    // TODO Restrict this to complete messages.
    pub fn unpack(&mut self) -> io::Result<String> {
        let mut complete_message = vec![];
        for chunk in self.chunks.drain() {
            complete_message.push_all(chunk.unwrap().payload.as_slice());
        }
        Ok(try!(unpack_complete(complete_message.as_slice())))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::cmp::min;
    use rand::{OsRng, Rng};
    use std::old_io::timer::sleep;
    use std::time::Duration;
    use time;
    use message::Chunk;

    #[test]
    fn single_chunk() {
        // Who knows if this should even be supported?
        let json = r#"{message":"foo","host":"bar","_utf8":"✓"}"#;

        let chunks = chunker(json, 100);
        let packet = chunks[0].as_slice();

        let chunk = Chunk::from_packet(packet).unwrap();
        let mut acc = ChunkAccumulator::new();
        let mut chunk_set = acc.accept(chunk).unwrap().unwrap();
        let result = chunk_set.unpack().unwrap();
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
        let mut chunk_set = acc.accept(chunk2).unwrap().unwrap();
        let result = chunk_set.unpack().unwrap();
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
        chunk1.arrival = time::get_time() - time::Duration::seconds(6);
        let chunk2 = Chunk::from_packet(packet2).unwrap();
        let mut acc = ChunkAccumulator::new();
        acc.accept(chunk1).unwrap();
        // Allow reaper thread to run - not bulletproof, but seems to work...
        sleep(Duration::milliseconds(10));

        let option = acc.accept(chunk2).unwrap();
        assert!(option.is_none(), "The first packet expired, so the second shouldn't complete anything");
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
        let mut chunk_set_a = acc.accept(chunk_a_2).unwrap().unwrap();
        let result_a = chunk_set_a.unpack().unwrap();
        assert_eq!(json_a, result_a.as_slice());
        let mut chunk_set_b = acc.accept(chunk_b_2).unwrap().unwrap();
        let result_b = chunk_set_b.unpack().unwrap();
        assert_eq!(json_b, result_b.as_slice());
    }

    fn chunker(message: &str, max_length: usize) -> Vec<Vec<u8>> {
        // Test only id.
        let mut id = [0u8; 8];
        let mut rng = OsRng::new().unwrap();
        rng.fill_bytes(&mut id);

        let length = min(message.len(), max_length);

        // Unsigned integer division ceiling.
        let count = (message.len() + length - 1) / length;

        // Limit to max (255 for u8, GELF is lower, but meh).
        // Panics otherwise, should return Result(Err).
        // @todo this truncates...
        let sequence_count = count as u8;
        let mut chunks = vec![];
        for x in 0..sequence_count {
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
