use crate::{
    sim::{await_completion, submit_write},
    store::{
        io::{CompleteIo, HandleIndex, IoCommand, IoKind},
        Page, PAGE_SIZE,
    },
    wal::{batch::Batch, record::Record},
};

use anyhow::bail;
use bitvec::{order::Msb0, view::AsBits};
use crossbeam_channel::{Receiver, Sender};
use std::os::fd::AsRawFd;

use std::{
    backtrace,
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
};

mod batch;
mod entry;
mod record;

// WAL format:
//
// series of 256KB records
// each record contains a sequence number, a checksum, a number of entries, and the entries
// themselves. entries never span records.
//
// Record format:
//   sequence number (8 bytes)
//   checksum (4 bytes) (magic number ++ data)
//   entry count (2 bytes)
//   data: [Entry]
//
// Entry format:
// kind: u8
//   1 => updated bucket
//   2 => cleared bucket
// data: (variable based on kind)
//  bucket update:
//    page ID (16 bytes)
//    diff (16 bytes)
//    changed [[u8; 32]] (len with diff)
//    bucket index (8 bytes)
//  bucket cleared:
//    bucket index (8 bytes)
//
// Multiple records are expected to have the same sequence number if they are part of the same
// transaction.
// e.g. [0 0 0 1 1 2 2 2 2 3 3 4 4 4 4] might be the order of sequence numbers in a WAL.
// this can be used to compare against the sequence number in the database's metadata file to
// determine which WAL entries need to be reapplied.
//
// WAL entries imply writing buckets and the meta-map. when recovering from a crash, do both.
//
//
// ASSUMPTIONS, TO BE DISCUSSED:
// + the file is expected to be contiguous in memory, so the last record starts at position
//   file.len() - WAL_RECORD_SIZE
// + All records must have a sequence number equal to the previous one or the previous one increased by 1,
//   the first record in the file defines the start.
//   (if we decide to use a ring wall, then the start could be somewhere else)
//   No gaps are accepted between records; otherwise, integrity is not maintained
// + We exect that the entries in the Records are locically correct

const WAL_RECORD_SIZE: usize = 256 * 1024;
// walawala
const WAL_CHECKSUM_MAGIC: u32 = 0x00a10a1a;

const PAGE_PER_RECORD: usize = WAL_RECORD_SIZE / PAGE_SIZE;

pub struct Wal {
    wal_file: File,
    // pointer to the page right after the last record
    // in the wal file
    //pos: u64,
    page_index: u64,
    last_batch: Option<Batch>,
}

impl Wal {
    /// Open a WAL file if it exists, otherwise, create a new empty one.
    ///
    /// If it already exists, all records will be parsed to verify the
    /// WAL's integrity. If not satisfied, an error will be returned.
    ///
    /// NOTE: This does not perform any checks on the database's consistency (yet),
    /// it needs to be done after creating the WAL and once we are sure it is correct.
    pub fn open(path: PathBuf) -> anyhow::Result<Self> {
        let mut wal_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        // if the wal file is empty no integrity check needs to be done
        let wal_file_len = wal_file.metadata()?.len();
        if wal_file_len == 0 {
            //println!("EMPTY");
            return Ok(Self {
                wal_file,
                page_index: 0,
                last_batch: None,
            });
        }

        // Scan records, check for singular integrity and group them
        // The last group of records is joined into the last batch that could possibly be re-applied

        let mut pos = 0;
        let expected_last_record_pos = wal_file_len - WAL_RECORD_SIZE as u64;

        let mut last_group_records = vec![];
        let mut sequence_number = None;

        while pos <= dbg!(expected_last_record_pos) {
            let mut raw_record = [0; WAL_RECORD_SIZE];
            wal_file
                .read_exact(&mut raw_record)
                .map_err(|_| anyhow::anyhow!("Incorrect Record size"))?;

            let record = Record::from_bytes(raw_record)?;

            match sequence_number {
                None if pos == 0 => sequence_number = Some(record.sequence_number()),
                None => unreachable!(),
                Some(sn) if sn + 1 == record.sequence_number() => {
                    last_group_records = vec![];
                    sequence_number = Some(record.sequence_number())
                }
                _ => (),
            };

            last_group_records.push(record);
            pos += WAL_RECORD_SIZE as u64;
        }

        Ok(Self {
            wal_file,
            page_index: pos * PAGE_PER_RECORD as u64,
            last_batch: Some(Batch::from_records(last_group_records)),
        })
    }

    pub fn last_sequence_number(&self) -> u64 {
        match self.last_batch {
            Some(ref batch) => batch.sequence_number(),
            None => 0,
        }
    }

    // apply last batch to WAL file
    pub fn apply_batch(
        &mut self,
        io_sender: &Sender<IoCommand>,
        io_receiver: &Receiver<CompleteIo>,
        io_handle_index: HandleIndex,
        batch: Batch,
    ) -> anyhow::Result<()> {
        dbg!(self.wal_file.metadata()?.len());
        let records: Vec<_> = batch.to_records();
        self.last_batch = Some(batch);

        let mut submitted = 0;
        let mut completed = 0;

        let command = IoCommand {
            kind: IoKind::Fallocate(
                self.wal_file.as_raw_fd(),
                dbg!(self.page_index),
                (records.len() * PAGE_PER_RECORD) as u64,
            ),
            handle: io_handle_index,
            user_data: 0, // unimportant
        };
        submitted += 1;
        submit_write(io_sender, io_receiver, command, &mut completed);
        await_completion(io_receiver, &mut completed);

        for record in records {
            let raw_record = record.to_bytes();
            println!("new recordd");

            let mut offset = 0;
            for _ in 0..PAGE_PER_RECORD {
                let mut page = Page::zeroed();
                page.copy_from_slice(&raw_record[offset..offset + PAGE_SIZE]);
                let command = IoCommand {
                    kind: IoKind::Write(self.wal_file.as_raw_fd(), self.page_index, Box::new(page)),
                    handle: io_handle_index,
                    user_data: 0, // unimportant.
                };

                submitted += 1;
                self.page_index += 1;

                submit_write(io_sender, io_receiver, command, &mut completed);

                offset += PAGE_SIZE;
            }
        }

        while completed < submitted {
            await_completion(io_receiver, &mut completed);
        }

        let command = IoCommand {
            kind: IoKind::Fsync(self.wal_file.as_raw_fd()),
            handle: io_handle_index,
            user_data: 0, // unimportant
        };

        submit_write(io_sender, io_receiver, command, &mut completed);

        submitted += 1;
        while completed < submitted {
            await_completion(io_receiver, &mut completed);
        }

        Ok(())
    }

    // pub fn store_last_batch(
    //     &self,
    //     io_handle_index: usize,
    //     io_sender: &Sender<IoCommand>,
    //     io_receiver: &Receiver<CompleteIo>,
    // ) -> usize {
    //      let mut submitted = 0;
    //      let mut completed = 0;

    //      let Some(ref last_batch) = self.last_batch else {
    //          panic!("Attempt to apply no existing batch")
    //      };

    //      for (bucket, page) in last_batch.pages.iter() {
    //          let command = IoCommand {
    //              // TODO: does it make sense to move the page to be an Arc?
    //              // So it is dropped when both the write finishes and the Wal drop the batch
    //              kind: IoKind::Write(PageIndex::Data(*bucket), page.clone()),
    //              handle: io_handle_index,
    //              user_data: 0, // unimportant.
    //          };

    //          submitted += 1;

    //          submit_write(io_sender, io_receiver, command, &mut completed);
    //      }

    //      for (index, page) in last_batch.meta_pages.iter() {
    //          let command = IoCommand {
    //              kind: IoKind::Write(PageIndex::MetaBytes(*index), page.clone()),
    //              handle: io_handle_index,
    //              user_data: 0, // unimportant
    //          };

    //          submitted += 1;

    //          submit_write(io_sender, io_receiver, command, &mut completed);
    //      }

    //      while completed < submitted {
    //          await_completion(io_receiver, &mut completed);
    //      }

    //      let command = IoCommand {
    //          kind: IoKind::Fsync,
    //          handle: io_handle_index,
    //          user_data: 0, // unimportant
    //      };

    //      let submit_write = submit_write(io_sender, io_receiver, command, &mut completed);

    //      submitted += 1;
    //      while completed < submitted {
    //          await_completion(io_receiver, &mut completed);
    //      }
    //     completed
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::io::{start_io_worker, Mode as IoMode};
    use crate::wal::record::tests::create_record;

    #[test]
    fn wal_create_save_and_open() {
        let _ = std::fs::remove_file("wal_test");
        let mut wal = Wal::open("wal_test".into()).unwrap();

        let (io_sender, io_receivers) = start_io_worker(1, IoMode::Real { num_rings: 1 });

        let records = [156..256, 1056..1156, 888..1000]
            .into_iter()
            .map(|r| create_record(164, r))
            .collect();
        let batch = Batch::from_records(records);

        wal.apply_batch(&io_sender, &io_receivers[0], 0, batch)
            .unwrap();

        let records = [164..250, 16..116, 400..600]
            .into_iter()
            .map(|r| create_record(164, r))
            .collect();
        let last_batch = Batch::from_records(records);

        wal.apply_batch(&io_sender, &io_receivers[0], 0, last_batch.clone())
            .unwrap();

        drop(wal);

        let wal = Wal::open("wal_test".into()).unwrap();
        assert_eq!(last_batch, wal.last_batch.unwrap());
        let _ = std::fs::remove_file("wal_test");
    }
}
