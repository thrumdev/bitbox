use crossbeam_channel::{Receiver, Sender};

use crate::store::{
    io::{CompleteIo, IoCommand, IoKind, PageIndex},
    Page, PAGE_SIZE,
};

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
};

use super::{await_completion, submit_write};

// TODO: handle batch eviction
pub struct Wal {
    wal_file: File,
    pos: u64,
    last_batch: Option<Batch>,
    n_batches: u64, //sequence_number: u64,
}

pub struct Batch {
    // key_value pairs
    // TODO: intead of saving the entire page we whould save the PageDiff
    // along with the vector of new pages
    pub pages: Vec<(u64, Box<Page>)>,
    pub meta_pages: Vec<(u64, Box<Page>)>,
}

impl Batch {
    pub fn new() -> Self {
        Batch {
            pages: vec![],
            meta_pages: vec![],
        }
    }
}

impl Wal {
    /// TODO: Open a wal and check for the integrity of
    /// the last batch of read, write
    pub fn open(path: PathBuf) -> anyhow::Result<Self> {
        if !std::path::Path::exists(&path) {
            let mut wal_file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)?;
            wal_file.write_all(&0u64.to_le_bytes())?;

            return Ok(Self {
                pos: wal_file
                    .stream_position()
                    .expect("Error extrating wal seek position"),
                wal_file,
                last_batch: None,
                n_batches: 0,
            });
        }

        let mut wal_file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut read_u64 = |file: &mut File| -> anyhow::Result<u64> {
            let mut buf = [0u8; 8];
            file.read_exact(&mut buf)?;
            Ok(u64::from_le_bytes(buf))
        };

        // firt thing in the wal file is the total number of batches saved
        //
        // TODO: this could become an entire page of wal metadata, or could be
        // moved into a manifest with other info
        let n_batches = read_u64(&mut wal_file)?;

        for _ in 0..n_batches - 1 {
            let n_pages = read_u64(&mut wal_file)?;
            wal_file.seek(std::io::SeekFrom::Current(
                n_pages as i64 * PAGE_SIZE as i64,
            ))?;

            let n_meta_pages = read_u64(&mut wal_file)?;
            wal_file.seek(std::io::SeekFrom::Current(
                n_meta_pages as i64 * PAGE_SIZE as i64,
            ))?;
        }

        // load the last batch into memory, if there is some discrepancy
        // this will need to re applied to storage
        let n_pages = read_u64(&mut wal_file)?;
        let mut pages = vec![];
        for _ in 0..n_pages {
            let bucket = read_u64(&mut wal_file)?;
            let mut page = Page::zeroed();
            wal_file.read_exact(&mut page)?;
            pages.push((bucket, Box::new(page)));
        }

        let n_meta_pages = read_u64(&mut wal_file)?;
        let mut meta_pages = vec![];
        for _ in 0..n_meta_pages {
            let index = read_u64(&mut wal_file)?;
            let mut meta_page = Page::zeroed();
            wal_file.read_exact(&mut meta_page)?;
            meta_pages.push((index, Box::new(meta_page)));
        }

        Ok(Self {
            pos: wal_file
                .stream_position()
                .expect("Error extrating wal seek position"),
            last_batch: Some(Batch { pages, meta_pages }),
            wal_file,
            n_batches,
        })
    }

    // apply last batch to WAL file
    pub fn apply_batch(&mut self, batch: Batch) -> anyhow::Result<()> {
        self.wal_file
            .write_all(&(batch.pages.len() as u64).to_le_bytes())?;

        for (bucket, page) in batch.pages.iter() {
            self.wal_file.write_all(&bucket.to_le_bytes())?;
            self.wal_file.write_all(&page)?;
        }

        self.wal_file
            .write_all(&(batch.meta_pages.len() as u64).to_le_bytes())?;

        for (index, page) in batch.meta_pages.iter() {
            self.wal_file.write_all(&index.to_le_bytes())?;
            self.wal_file.write_all(&page)?;
        }

        self.last_batch = Some(batch);
        self.n_batches += 1;
        self.pos = self.wal_file.stream_position()?;

        self.wal_file.seek(std::io::SeekFrom::Start(0))?;
        self.wal_file.write_all(&self.n_batches.to_le_bytes())?;
        self.wal_file.seek(std::io::SeekFrom::Start(self.pos))?;

        self.wal_file.sync_all()?;

        Ok(())
    }

    pub fn store_last_batch(
        &self,
        io_handle_index: usize,
        io_sender: &Sender<IoCommand>,
        io_receiver: &Receiver<CompleteIo>,
    ) -> usize {
        let mut submitted = 0;
        let mut completed = 0;

        let Some(ref last_batch) = self.last_batch else {
            panic!("Attempt to apply no existing batch")
        };

        for (bucket, page) in last_batch.pages.iter() {
            let command = IoCommand {
                // TODO: does it make sense to move the page to be an Arc?
                // So it is dropped when both the write finishes and the Wal drop the batch
                kind: IoKind::Write(PageIndex::Data(*bucket), page.clone()),
                handle: io_handle_index,
                user_data: 0, // unimportant.
            };

            submitted += 1;

            submit_write(io_sender, io_receiver, command, &mut completed);
        }

        for (index, page) in last_batch.meta_pages.iter() {
            let command = IoCommand {
                kind: IoKind::Write(PageIndex::MetaBytes(*index), page.clone()),
                handle: io_handle_index,
                user_data: 0, // unimportant
            };

            submitted += 1;

            submit_write(io_sender, io_receiver, command, &mut completed);
        }

        while completed < submitted {
            await_completion(io_receiver, &mut completed);
        }

        let command = IoCommand {
            kind: IoKind::Fsync,
            handle: io_handle_index,
            user_data: 0, // unimportant
        };

        submit_write(io_sender, io_receiver, command, &mut completed);

        submitted += 1;
        while completed < submitted {
            await_completion(io_receiver, &mut completed);
        }
        completed
    }
}
