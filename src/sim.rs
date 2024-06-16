//! Simulation meant to emulate the workload of a merkle trie like NOMT in editing pages.
//!
//! We have a number of pages which are "in" the store, given by the range `0..num_pages`.
//! Workers each operate over pages with a unique prefix and randomly sample from this range
//! to choose pages to load, and randomly sample from `num_pages..` with `cold_rate` probability.
//! 
//! Pages are always loaded in batches of `preload_count` and an extra page is fetched after this
//! with `load_extra_rate` probability. if `load_extra_rate` is zero it is like saying we always
//! expect to find a leaf after loading `preload_count` pages in NOMT. realistically, it should be
//! low but non-zero.
//!
//! Pages consist of the page ID followed by 126 32-byte vectors some of which are chosen to be
//! randomly updated at a rate of `page_item_update_rate`.
//!
//! Workers first create their workload, then read, update, synchronize, and write. They query from
//! the underlying store with hash-table semantics, and may need multiple round trips in the case
//! of collisions.

use crossbeam_channel::{Sender, Receiver};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use crate::meta_map::MetaMap;
use crate::store::{io::{self as store_io, IoKind, IoCommand, CompleteIo}, Store, Page};

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

pub fn run_simulation(store: Arc<Store>, mut params: Params, meta_bytes: MetaMap) {
    params.num_pages /= params.num_workers;
    params.workload_size /= params.num_workers;

    let meta_bytes = Arc::new(RwLock::new(meta_bytes));
    let (io_sender, io_receivers) = store_io::start_io_worker(store.clone(), params.num_workers);
    for (i, io_receiver) in io_receivers.into_iter().enumerate() {
        let store = store.clone();
        let io_sender = io_sender.clone();
        let barrier = barrier.clone();
        let meta_bytes = meta_bytes.clone();
        std::thread::spawn(move || run_worker(i, io_sender, io_receiver, params, meta_bytes));
    }
}

fn run_worker(
    worker_index: usize, 
    io_sender: Sender<IoCommand>,
    io_receiver: Receiver<IoCommand>,
    params: Params,
    meta_bytes: Arc<RwLock<MetaMap>>,
) {
    let mut rng = rand::thread_rng();

    loop {
        // 1. create workload - (preload_count + 1) * workload_size random "page IDs"
        let workload_pages = (0..params.workload_size).flat_map(|i| {
            // sample from typical range if not cold, out of range if cold
            let range = if rng.gen::<f32>() < params.cold_rate {
                params.num_pages..
            } else {
                ..params.num_pages
            };

            // turn each sample into a random hash, set a unique byte per worker to discriminate.
            (0..params.preload_count + 1)
                .map(move |_| rng.gen_range(range))
                .map(|sample| blake3::hash(sample.to_le_bytes().as_slice()))
                .map(|hash| {
                    let mut page_id = [0; 16];
                    page_id.copy_from_slice(hash.as_bytes[..16]);
                    page_id[0] = params.worker_index as u8;
                    page_id
                })
        }).collect::<Vec<_>>();

        // 2. read
        {
            let meta_bytes = meta_bytes.read().unwrap();
            read_phase(
                unimplemented!(),
                &*meta_bytes,
                &params,
                workload_pages,
                &io_sender,
                &io_receiver,
            );
        }

        // 3. write
        {
            let mut meta_bytes = meta_bytes.write().unwrap();
        }
    }
}

type BucketIndex = usize;

#[derive(Clone, Copy)]
struct ProbeSequence {
    bucket: usize,
    step: usize,
}

enum PageState {
    Unneeded,
    Pending {
        probe: ProbeSequence,
    },
    Submitted {
        probe: ProbeSequence,
    },
    Received {
        location: Option<BucketIndex>, // `None` if fresh
        page: Box<Page>,
    }
}

impl PageState {
    fn is_unneeded(&self) -> bool {
        match self {
            PageState::Unneeded => true,
            _ => false,
        }
    }
}

struct ReadState {
    pages: Vec<(PageId, PageState)>,
    job: u32,
}

impl ReadState {
    fn all_ready(&self) -> bool {
        self.pages.iter().all(|(_, state)| match state {
            State::Unneeded | State::Received { .. } => true,
            _ => false,
        })
    }

    fn next_pending(&mut self) -> Option<(usize, BucketIndex)> {
        self.page_ids.iter().filter_map(|(page, state)| match state {
            PageState::Pending { probe } => Some((page, probe.bucket)),
            _ => None
        }).next()
    }

    fn set_page_state(&mut self, index: usize, page_state: PageState) {
        self.pages[i].1 = page_state;
    }

    fn page_id(&self, index: usize) -> PageId {
        self.pages[i].0
    }
}

fn read_phase(
    salt: &[u8],
    meta_bytes: &MetaMap,
    params: &Params,
    workload: Vec<PageId>,
    io_sender: &Sender<IoCommand>,
    io_receiver: &Receiver<CompleteIo>,
) {
    const MAX_IN_FLIGHT: usize = 16;

    let mut rng = rand::thread_rng();

    // contains pages which are in-flight or received.
    let mut in_flight: VecDeque<ReadState> = VecDeque::with_capacity(MAX_IN_FLIGHT);

    let pack_user_data = |job: u32, index: u32| (job as u64) << 32 + index as u32;
    let unpack_user_data = |user_data: u64| (user_data >> 32 as u32, user_data & 0x00000000FFFFFFFF as u32);

    // we process our workload sequentially but look forward and attempt to keep the I/O saturated
    // this means we need to keep a current index we are working on while probing
    let mut cur_job = 0;
    loop {
        // handle complete I/O
        for complete_io in io_receiver.try_iter() {
            complete_io.result.expect("I/O failed");
            let command = complete_io.command;

            let (job_idx, index_in_job) = unpack_user_data(command.user_data);
            let front_job = self.in_flight[0].job;
            let job = &mut self.in_flight[job_idx - front_job];
            let expected_page = job.page_id(index_in_job);

            // check that page idx matches the fetched page.
            if page_id_matches(&*command.page) {
                job.set_page_state(index_in_job, PageState::Received {
                    location: Some(unimplemented!())
                });
            } else {
                // TODO probe again and set pending.
            }
        }

        // submit requests from in-flight batches.
        let mut can_submit = true; 
        'a:
        for batch in self.in_flight.iter_mut() {
            while let Some((page_id, probe)) = batch.next_pending() {
                // TODO submit I/O
                // set state to submitted if successful
                // break outer loop if paused and set can_submit to false.
            }
        }

        // add in-flight batches.
        if can_submit && in_flight.len() < MAX_IN_FLIGHT {
            let start = cur_job * (params.preload_count + 1);
            let end = start + params.preload_count + 1;

            if end > workload.len() && in_flight.is_empty() {
                break
            }

            let job_idx = cur_job + in_flight.len();
            in_flight.push_back(ReadState {
                pages: workload[start..end].iter().cloned().enumerate().map(|(i, page_id)| {
                    if i == params.preload_count {
                        return PageState::Unneeded
                    }

                    // find probe and set as pending.
                })
            });
        }

        while in_flight.front().map_or(false, |state| state.ready()) {
            let mut item = in_flight.pop_front().unwrap();
            if item.pages.last().unwrap().is_unneeded() && rng.gen::<f32>() < params.load_extra_rate {
                // probe and set as pending.
                in_flight.push_front(item);
                break
            }

            cur_job += 1;
            // TODO update randomly and record changes.
        }
    }
}

fn page_id_matches(page: &Page, expected_page_id: &PageId) {
    &page[..expected_page_id.len()] == expected_page_id.as_slice()
}