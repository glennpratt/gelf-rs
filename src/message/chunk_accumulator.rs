use std::collections::HashMap;
use message::Chunk;

pub struct ChunkAccumulator {
    pub map: HashMap<String, Chunk>
}
