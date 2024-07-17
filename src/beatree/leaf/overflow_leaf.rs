// Here is the layout of an overflow leaf node:
//
// ```rust,ignore
// n: 0u16
// key: [u8; 32]
// size: u64 // size of the value in bytes
// value: [u8]
// ```
//
// TODO: add comments and correct
// The first page will store portion of the values, precisely PAGE_SIZE - 42 bytes.
// All other bytes will be saved in other store pages

use anyhow::bail;

use crate::store::Page;

use super::PageNumber;

pub struct OveflowLeaf {
    front: Box<Page>,
    page_number: PageNumber,
}

impl OveflowLeaf {
    pub fn try_from(pn: PageNumber, page: Box<Page>) -> anyhow::Result<Self> {
        let leaf = Self {
            front: page,
            page_number: pn,
        };
        if leaf.n() == 0 {
            bail!("This is not an Encoded Leaf")
        }
        Ok(leaf)
    }

    fn n(&self) -> usize {
        u16::from_le_bytes(self.front[0..2].try_into().unwrap()) as usize
    }

    pub fn page(self) -> Vec<Box<Page>> {
        todo!()
    }

    pub fn insert(&mut self, key: Key, value: Vec<u8>) -> LeafInsertResult {
        todo!()
    }

    pub fn remove(&mut self, key: Key) {
        todo!()
    }

    pub fn get(&self, key: &Key) -> Option<&[u8]> {
        todo!()
    }
}
