use crate::beatree::leaf::free_list::FreeList;
use crate::beatree::leaf::{node::LeafNode, PageNumber};
use crate::io::{CompleteIo, IoCommand, IoKind};
use crate::store::{Page, PAGE_SIZE};
use crossbeam_channel::{Receiver, Sender, TrySendError};

use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;

pub struct LeafStore {
    store_file: File,
    io_handle_index: usize,
    io_sender: Sender<IoCommand>,
    io_receiver: Receiver<CompleteIo>,
    // monotonic page number, used when the free list is empty
    nonce_page_number: PageNumber,
    // this is the max supported page number, after which write fails,
    // `ftruncate` of the file, to make it bigger, must be handled by upper layers
    max_page_number: PageNumber,
    free_list: FreeList,
}

impl LeafStore {
    /// creates a LeafStore over a possibly already existing File
    pub fn create(
        fd: File,
        free_list_head: Option<PageNumber>,
        nonce_page_number: PageNumber,
        io_handle_index: usize,
        io_sender: Sender<IoCommand>,
        io_receiver: Receiver<CompleteIo>,
    ) -> LeafStore {
        let file_size = fd
            .metadata()
            .expect("Error extracting metadata from LeafStore file")
            .size() as usize;

        let max_page_number = PageNumber((file_size / PAGE_SIZE) as u32);

        Self {
            free_list: FreeList::new(
                &fd,
                &io_sender,
                io_handle_index,
                &io_receiver,
                free_list_head,
            ),
            store_file: fd,
            io_handle_index,
            io_sender,
            io_receiver,
            nonce_page_number,
            max_page_number,
        }
    }

    /// Returns the leaf page with the specified page number.
    pub fn query(&self, pn: PageNumber) -> LeafNode {
        let page = Box::new(Page::zeroed());

        let mut command = Some(IoCommand {
            kind: IoKind::Read(self.store_file.as_raw_fd(), pn.0 as u64, page),
            handle: self.io_handle_index,
            user_data: 0,
        });

        while let Some(c) = command.take() {
            match self.io_sender.try_send(c) {
                Ok(()) => break,
                Err(TrySendError::Disconnected(_)) => panic!("I/O leaf store worker dropped"),
                Err(TrySendError::Full(c)) => {
                    command = Some(c);
                }
            }
        }

        // wait for completion
        let completion = self
            .io_receiver
            .recv()
            .expect("I/O leaf store worker dropped");
        assert!(completion.result.is_ok());
        let page = completion.command.kind.unwrap_buf();

        LeafNode { inner: page }
    }

    // create a LeafStoreTx able to append and release leaves from the LeafStore
    pub fn start_tx<'a>(&'a mut self) -> LeafStoreTx {
        LeafStoreTx {
            nonce_page_number: &mut self.nonce_page_number,
            max_page_number: self.max_page_number,
            free_list: &mut self.free_list,
            to_allocate: vec![],
            released: vec![],
            exceeded: vec![],
        }
    }

    // updates the mex supported page number based on the leaf store file size
    pub fn update_size(&mut self) {
        let file_size = self
            .store_file
            .metadata()
            .expect("Error extracting metadata from LeafStore file")
            .size() as usize;

        self.max_page_number = PageNumber((file_size / PAGE_SIZE) as u32);
    }

    // perform writes for all provided pages, then waits till the end
    //
    // panics if the provided PageNumber is bigger then the max supported one
    pub fn write(&mut self, pages: Vec<(PageNumber, Box<Page>)>) {
        let n_write_requests = pages.len();
        for (pn, page) in pages {
            let mut command = Some(IoCommand {
                kind: IoKind::Write(self.store_file.as_raw_fd(), pn.0 as u64, page),
                handle: self.io_handle_index,
                user_data: 0,
            });

            while let Some(c) = command.take() {
                match self.io_sender.try_send(c) {
                    Ok(()) => break,
                    Err(TrySendError::Disconnected(_)) => panic!("I/O leaf store worker dropped"),
                    Err(TrySendError::Full(c)) => {
                        command = Some(c);
                    }
                }
            }
        }

        // wait for completion of all write commands
        for _ in 0..n_write_requests {
            let completion = self
                .io_receiver
                .recv()
                .expect("I/O leaf store worker dropped");
            assert!(completion.result.is_ok());
        }
    }
}

pub struct LeafStoreTx<'a> {
    nonce_page_number: &'a mut PageNumber,
    max_page_number: PageNumber,
    free_list: &'a mut FreeList,
    released: Vec<PageNumber>,
    to_allocate: Vec<(PageNumber, LeafNode)>,
    exceeded: Vec<(PageNumber, LeafNode)>,
}

struct LeafStoreTxOutput {
    // contains pages that can already be written in the LeafStore
    to_allocate: Vec<(PageNumber, Box<Page>)>,
    // contains pages that require the LeafStore to increase
    exceeded: Vec<(PageNumber, Box<Page>)>,
    new_free_list_head: PageNumber,
}

impl<'a> LeafStoreTx<'a> {
    pub fn allocate(&mut self, leaf_page: LeafNode) -> PageNumber {
        let leaf_pn = match self.free_list.pop() {
            Some(pn) => pn,
            None => {
                let pn = *self.nonce_page_number;
                self.nonce_page_number.0 += 1;
                pn
            }
        };

        if leaf_pn.0 < self.max_page_number.0 {
            self.to_allocate.push((leaf_pn, leaf_page));
        } else {
            self.exceeded.push((leaf_pn, leaf_page));
        }
        leaf_pn
    }

    pub fn release(&mut self, id: PageNumber) {
        self.released.push(id);
    }

    // commits the changes creating a set of pages that needs to be written into the LeafStore
    pub fn commit(mut self) -> LeafStoreTxOutput {
        let free_list_output = self.free_list.encode(
            self.released,
            &mut self.nonce_page_number,
            self.max_page_number,
        );

        let mut to_allocate = self
            .to_allocate
            .into_iter()
            .map(|(pn, leaf_page)| (pn, leaf_page.inner))
            .collect::<Vec<_>>();
        to_allocate.extend(
            free_list_output
                .to_allocate
                .into_iter()
                .map(|(pn, free_list_page)| (pn, free_list_page.inner)),
        );

        let mut exceeded = self
            .exceeded
            .into_iter()
            .map(|(pn, leaf_page)| (pn, leaf_page.inner))
            .collect::<Vec<_>>();
        exceeded.extend(
            free_list_output
                .exceeded
                .into_iter()
                .map(|(pn, free_list_page)| (pn, free_list_page.inner)),
        );

        LeafStoreTxOutput {
            to_allocate,
            exceeded,
            // after appending to the free list, the head will always be present
            new_free_list_head: self.free_list.head_pn().unwrap(),
        }
    }
}
