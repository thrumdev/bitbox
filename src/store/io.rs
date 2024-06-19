use super::{Page, Store, PAGE_SIZE};
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use io_uring::{cqueue, opcode, squeue, types, IoUring};
use slab::Slab;
use std::{
    os::fd::AsRawFd,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const RING_CAPACITY: u32 = 32;

// max number of inflight requests is bounded by the slab.
const MAX_IN_FLIGHT: usize = 32;

static IO_OPS: AtomicU64 = AtomicU64::new(0);

pub type HandleIndex = usize;

#[derive(Clone)]
pub enum IoKind {
    Read(PageIndex, Box<Page>),
    Write(PageIndex, Box<Page>),
    Fsync,
}

impl IoKind {
    pub fn unwrap_buf(self) -> Box<Page> {
        match self {
            IoKind::Read(_, buf) | IoKind::Write(_, buf) => buf,
            IoKind::Fsync => panic!("attempted to extract buf from fsync"),
        }
    }
}

#[derive(Clone, Copy)]
pub enum PageIndex {
    Data(u64),
    MetaBytes(u64),
    Meta,
}

impl PageIndex {
    fn index_in_store(self, store: &Store) -> u64 {
        match self {
            PageIndex::Data(i) => store.data_page_offset() + i,
            PageIndex::MetaBytes(i) => 1 + i,
            PageIndex::Meta => 0,
        }
    }
}

pub struct IoCommand {
    pub kind: IoKind,
    pub handle: HandleIndex,
    // note: this isn't passed to io_uring, it's higher-level userdata.
    pub user_data: u64,
}

pub struct CompleteIo {
    pub command: IoCommand,
    pub result: std::io::Result<()>,
}

struct PendingIo {
    command: IoCommand,
    start: Instant,
}

pub fn total_io_ops() -> u64 {
    IO_OPS.load(Ordering::Relaxed)
}

/// Create an I/O worker managing an io_uring and sending responses back via channels to a number
/// of handles.
pub fn start_io_worker(
    store: Arc<Store>,
    num_handles: usize,
) -> (Sender<IoCommand>, Vec<Receiver<CompleteIo>>) {
    // main bound is from the pending slab.
    let (command_tx, command_rx) = crossbeam_channel::bounded(MAX_IN_FLIGHT * 2);
    let (handle_txs, handle_rxs) = (0..num_handles)
        .map(|_| crossbeam_channel::unbounded())
        .unzip();
    let _ = std::thread::Builder::new()
        .name("io_worker".to_string())
        .spawn(move || run_worker(store, command_rx, handle_txs))
        .unwrap();
    (command_tx, handle_rxs)
}

fn run_worker(
    store: Arc<Store>,
    command_rx: Receiver<IoCommand>,
    handle_tx: Vec<Sender<CompleteIo>>,
) {
    let mut pending: Slab<PendingIo> = Slab::with_capacity(MAX_IN_FLIGHT);

    let mut ring = IoUring::<squeue::Entry, cqueue::Entry>::builder()
        .build(RING_CAPACITY)
        .expect("Error building io_uring");

    let (submitter, mut submit_queue, mut complete_queue) = ring.split();

    const LOG_DURATION: Duration = Duration::from_millis(200);

    let mut iter = 0;
    let mut last_log = Instant::now();
    let mut total_inflight_us = 0;
    let mut total_syscall_ns = 0;
    let mut total_complete_ns = 0;
    let mut total_submit_ns = 0;
    let mut completions = 0;
    let mut arrivals = 0;
    let mut empty_count = 0;
    let mut full_count = 0;
    loop {   
        let elapsed = last_log.elapsed();
        if elapsed > LOG_DURATION {
            last_log = Instant::now();
            let arrival_rate = arrivals as f64 * 1000.0 / elapsed.as_millis() as f64;
            let average_inflight = total_inflight_us as f64 / completions as f64;
            println!("full={full_count} empty={empty_count} iterations of {iter}");
            println!("syscall_time={}ns completion_time={}ns submit_time={}ns", 
                (total_syscall_ns as f64 / iter as f64) as usize,
                (total_complete_ns as f64 / iter as f64) as usize,
                (total_submit_ns as f64 / iter as f64) as usize,
            );
            println!("arrivals={} (rate {}/s) completions={} avg_inflight={}us | {}ms",
                arrivals,
                arrival_rate as usize,
                completions,
                average_inflight as usize,
                elapsed.as_millis(),
            );
            println!("  estimated-QD={}", (arrival_rate * average_inflight / 1_000_000.0) as usize);

            total_inflight_us = 0;
            total_syscall_ns = 0;
            total_complete_ns = 0;
            total_submit_ns = 0;
            completions = 0;
            arrivals = 0;
            full_count = 0;
            empty_count = 0;
            iter = 0;
        }

        iter += 1;
        
        // 1. process completions.
        let complete_start = Instant::now();

        // uncomment to complete I/O after deadline instead of io-uring
        {
            const SEND_BACK_DURATION: Duration = Duration::from_micros(100);
            for i in 0..MAX_IN_FLIGHT {
                if pending.get(i).map_or(false, |i| i.start.elapsed() > SEND_BACK_DURATION) {
                    let inflight = pending.remove(i);
                    
                    completions += 1;
                    total_inflight_us += inflight.start.elapsed().as_micros();
                    
                    let command = inflight.command;
                    let handle_idx = command.handle;
                    let complete = CompleteIo { command: command, result: Ok(()) };
                    if let Err(_) = handle_tx[handle_idx].send(complete) {
                        // TODO: handle?
                        break;
                    }
                }
            }
        }

        if !pending.is_empty() {
            complete_queue.sync();
            while let Some(completion_event) = complete_queue.next() {
                if !pending.get(completion_event.user_data() as usize).is_some() {
                    continue
                }
                let PendingIo {
                    command,
                    start,
                } = pending.remove(completion_event.user_data() as usize);

                total_inflight_us += start.elapsed().as_micros();
                completions += 1;

                let handle_idx = command.handle;
                let result = if completion_event.result() == -1 {
                    Err(std::io::Error::from_raw_os_error(completion_event.result()))
                } else {
                    Ok(())
                };
                let complete = CompleteIo { command, result: Ok(()) };
                if let Err(_) = handle_tx[handle_idx].send(complete) {
                    // TODO: handle?
                    break;
                }
            }
        }
        total_complete_ns += complete_start.elapsed().as_nanos();

        // 2. accept new I/O requests when slab has space & submission queue is not full.
        let mut to_submit = false;
        let mut ios = 0;

        let submit_start = Instant::now();
        submit_queue.sync();
        if pending.len() == MAX_IN_FLIGHT {
            full_count += 1;
        } else if pending.is_empty() {
            empty_count += 1;
        }
        while pending.len() < MAX_IN_FLIGHT && !submit_queue.is_full() {
            let next_io = if pending.is_empty() {
                // block on new I/O if nothing in-flight.
                match command_rx.recv() {
                    Ok(command) => command,
                    Err(_) => break, // disconnected
                }
            } else {
                match command_rx.try_recv() {
                    Ok(command) => command,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break, // TODO: wait on pending I/O?
                }
            };
            arrivals += 1;

            // uncomment to test what happens if we don't I/O
            // {
            //     let handle_idx = next_io.handle;
            //     let complete = CompleteIo { command: next_io, result: Ok(()) };
            //     if let Err(_) = handle_tx[handle_idx].send(complete) {
            //         // TODO: handle?
            //         break;
            //     }

            //     break
            // }

            to_submit = true;
            let pending_index = pending.insert(PendingIo {
                command: next_io,
                start: Instant::now(),
            });

            let entry = submission_entry(
                &mut pending.get_mut(pending_index).unwrap().command,
                &*store,
            ).user_data(pending_index as u64);

            // unwrap: known not full
            unsafe { submit_queue.push(&entry).unwrap() };
            ios += 1;
        }

        // 3. submit all together.
        if to_submit {
            IO_OPS.fetch_add(ios, Ordering::Relaxed);
            submit_queue.sync();
        }

        total_submit_ns += submit_start.elapsed().as_nanos();

        let wait = if pending.len() == MAX_IN_FLIGHT {
            1
        } else {
            0
        };

        let syscall_start = Instant::now();
        submitter.submit_and_wait(wait).unwrap();
        total_syscall_ns += syscall_start.elapsed().as_nanos();
    }
}

fn submission_entry(command: &mut IoCommand, store: &Store) -> squeue::Entry {
    match command.kind {
        IoKind::Read(page_index, ref mut buf) => opcode::Read::new(
            types::Fd(store.store_file.as_raw_fd()),
            buf.as_mut_ptr(),
            PAGE_SIZE as u32,
        )
        .offset(page_index.index_in_store(store) * PAGE_SIZE as u64)
        .build(),
        IoKind::Write(page_index, ref buf) => opcode::Write::new(
            types::Fd(store.store_file.as_raw_fd()),
            buf.as_ptr(),
            PAGE_SIZE as u32,
        )
        .offset(page_index.index_in_store(store) * PAGE_SIZE as u64)
        .build(),
        IoKind::Fsync => opcode::Fsync::new(types::Fd(store.store_file.as_raw_fd()))
            .build(),
    }
}
