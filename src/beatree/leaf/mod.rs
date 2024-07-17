// The `LeafStore` struct manages leaves. It's responsible for management (allocation and
// deallocation) and querying the LNs by their LNID.
//
// It maintains an in-memory copy of the freelist to facilitate the page management. The allocation
// is performed in LIFO order. The allocations are performed in batches to amortize the IO for the
// freelist and metadata updates (growing the file in case freelist is empty).
//
// The leaf store doesn't perform caching. When querying the leaf store returns a handle to a page.
// As soon as the handle is dropped, the data becomes inaccessible and another disk roundtrip would
// be required to access the data again.
//
// Some naming conventions:
// + a Page is fixed amount of arbitrary bytes written and stored into the LeafStore
// + Node is a logical construct, used to describe a Leaf inside a tree strucutre as a node
// + while Encoded represents something on top of arbitrary bytes which provide some logical functionality
//

use crate::store::Page;

use self::{
    encoded_leaf::EncodedLeaf,
    overflow_leaf::{LeafOveflow, OveflowLeaf},
};

use super::Key;

mod encoded_leaf;
mod free_list;
mod overflow_leaf;
pub mod store;

pub enum LeafNode {
    Encoded(EncodedLeaf),
    Overflow(OveflowLeaf),
}

/// The page number of a page, either a Leaf or a Free List page, in
/// the LeafStore
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageNumber(u32);

impl PageNumber {
    pub fn is_nil(&self) -> bool {
        self.0 == 0
    }
}

#[derive(PartialEq, Debug)]
pub enum LeafInsertResult {
    Ok,
    NoSpaceLeft,
}

impl LeafNode {
    // If the page is an oveflow one then returns the number
    // of other pages required to fetch all the value
    pub fn new(pn: PageNumber, page: Box<Page>) -> LeafNode {
        // the mcd between LeafPage and LeafOveflow is the first 2 bytes
        // in the first page, if 0 it is an oveflow page

        let n = u16::from_le_bytes(page[0..2].try_into().unwrap()) as usize;

        if n != 0 {
            LeafNode::Encoded(EncodedLeaf::try_from(page).unwrap())
        } else {
            LeafNode::Overflow(OveflowLeaf::try_from(pn, page).unwrap())
        }
    }

    pub fn pages(self) -> Vec<Box<Page>> {
        match self {
            LeafNode::Encoded(e) => vec![e.page()],
            LeafNode::Overflow(o) => o.pages(),
        }
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
