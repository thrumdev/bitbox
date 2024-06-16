//! Simulation meant to emulate the workload of a merkle trie like NOMT in editing pages.
//!
//! We have a number of pages which are "in" the store, given by the range `0..num_pages`.
//! Workers each operate over pages with a unique prefix and randomly sample from this range
//! to choose pages to load, and randomly sample from `num_pages..` with `cold_rate` probability.
//!
//! The read workload is split across N workers (since they do some CPU work) and the write
//! workload happens on a single thread.
//!
//! The read workload involves reading pages and updating them, while the write workload involves
//! writing to a WAL and then doing random writes to update buckets / meta bits.
//!
//! Pages are always loaded in batches of `preload_count` and an extra page is fetched after this
//! with `load_extra_rate` probability. if `load_extra_rate` is zero it is like saying we always
//! expect to find a leaf after loading `preload_count` pages in NOMT. realistically, it should be
//! low but non-zero.
//!
//! Pages consist of the page ID followed by 126 32-byte vectors some of which are chosen to be
//! randomly updated at a rate of `page_item_update_rate`.

use ahash::RandomState;
use bitvec::prelude::*;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use rand::Rng;

use std::collections::VecDeque;
use std::sync::{Arc, Barrier, RwLock};

use crate::meta_map::MetaMap;
use crate::store::{
    io::{self as store_io, CompleteIo, IoCommand, IoKind},
    Page, Store, PAGE_SIZE,
};

mod read;

#[derive(Clone, Copy)]
pub struct Params {
    pub num_workers: usize,
    // TODO: num ioring workers
    pub num_pages: usize,
    pub workload_size: usize,
    pub cold_rate: f32,
    pub preload_count: usize,
    pub load_extra_rate: f32,
    pub page_item_update_rate: f32,
}

type PageId = [u8; 16];
type PageDiff = [u8; 16];

struct ChangedPage {
    page_id: PageId,
    bucket: Option<BucketIndex>,
    buf: Box<Page>,
    diff: PageDiff,
}

const SLOTS_PER_PAGE: usize = 126;

fn slot_range(slot_index: usize) -> std::ops::Range<usize> {
    let start = 32 + slot_index * 32;
    let end = slot_index + 32;
    start..end
}

fn make_hasher(seed: [u8; 32]) -> RandomState {
    let extract_u64 = |range| {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&seed[range]);
        u64::from_le_bytes(buf)
    };

    RandomState::with_seeds(
        extract_u64(0..8),
        extract_u64(8..16),
        extract_u64(16..24),
        extract_u64(24..32),
    )
}

pub fn run_simulation(store: Arc<Store>, mut params: Params, meta_map: MetaMap) {
    params.num_pages /= params.num_workers;
    params.workload_size /= params.num_workers;

    let meta_map = Arc::new(RwLock::new(meta_map));
    let (io_sender, mut io_receivers) =
        store_io::start_io_worker(store.clone(), params.num_workers + 1);
    let (page_changes_tx, page_changes_rx) = crossbeam_channel::unbounded();

    let write_io_receiver = io_receivers.pop().unwrap();

    let mut start_work_txs = Vec::new();
    for (i, io_receiver) in io_receivers.into_iter().enumerate() {
        let map = Map {
            io_sender: io_sender.clone(),
            io_receiver,
            hasher: make_hasher(store.seed()),
        };
        let meta_map = meta_map.clone();
        let page_changes_tx = page_changes_tx.clone();
        let (start_work_tx, start_work_rx) = crossbeam_channel::bounded(1);
        std::thread::spawn(move || {
            read::run_worker(i, params, map, meta_map, page_changes_tx, start_work_rx)
        });
        start_work_txs.push(start_work_tx);
    }

    let io_handle_index = params.num_workers;

    loop {
        let barrier = Arc::new(Barrier::new(params.num_workers + 1));
        for tx in &start_work_txs {
            let _ = tx.send(barrier.clone());
        }

        // wait for reading to be done.
        let _ = barrier.wait();

        let mut meta_map = meta_map.write().unwrap();
        write(
            &io_sender,
            &write_io_receiver,
            &page_changes_rx,
            &mut meta_map,
        );
    }
}

type BucketIndex = u64;

#[derive(Clone, Copy)]
struct ProbeSequence {
    hash: u64,
    bucket: u64,
    step: u64,
}

struct Map {
    io_sender: Sender<IoCommand>,
    io_receiver: Receiver<CompleteIo>,
    hasher: RandomState,
}

impl Map {
    fn begin_probe(&self, page_id: &PageId, meta_map: &MetaMap) -> ProbeSequence {
        let hash = self.hasher.hash_one(page_id);
        ProbeSequence {
            hash,
            bucket: hash % meta_map.len() as u64,
            step: 0,
        }
    }

    // search for the bucket the probed item may live in. returns `None` if it definitely does not
    // exist in the map. `Some` means it may exist in that bucket and needs to be probed.
    fn search(
        &self,
        meta_map: &MetaMap,
        mut probe_sequence: ProbeSequence,
    ) -> Option<ProbeSequence> {
        loop {
            probe_sequence.bucket += probe_sequence.step;
            probe_sequence.step += 1;
            probe_sequence.bucket %= meta_map.len() as u64;

            if meta_map.hint_empty(probe_sequence.bucket as usize) {
                return None;
            }

            if meta_map.hint_not_match(probe_sequence.bucket as usize, probe_sequence.hash) {
                continue;
            }
            return Some(probe_sequence);
        }
    }

    // search for the first empty bucket along a probe sequence.
    fn search_free(&self, meta_map: &MetaMap, mut probe_sequence: ProbeSequence) -> u64 {
        loop {
            probe_sequence.bucket += probe_sequence.step;
            probe_sequence.step += 1;
            probe_sequence.bucket %= meta_map.len() as u64;

            if meta_map.hint_empty(probe_sequence.bucket as usize)
                || meta_map.hint_tombstone(probe_sequence.bucket as usize)
            {
                return probe_sequence.bucket;
            }
        }
    }
}

fn write(
    io_sender: &Sender<IoCommand>,
    io_receiver: &Receiver<CompleteIo>,
    changed_pages: &Receiver<ChangedPage>,
    meta_map: &mut MetaMap,
) {
    // TODO
}
