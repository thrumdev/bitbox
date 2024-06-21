use crate::wal::entry::Entry;
use crate::wal::WAL_CHECKSUM_MAGIC;
use crate::wal::WAL_RECORD_SIZE;

use anyhow::bail;

// Item stored in WAL, mult
#[derive(Clone, PartialEq, Debug)]
pub struct Record {
    sequence_number: u64,
    //checksum: u32,
    //entry_count: u16,
    data: Vec<Entry>,
}

impl Record {
    // panics if the size of the record is bigger then WAL_RECORD_SIZE
    pub fn new(sequence_number: u64, data: Vec<Entry>) -> Self {
        let record = Self {
            sequence_number,
            //checksum: todo!(),
            //entry_count: todo!(),
            data,
        };

        //TODO check size
        record
    }

    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    pub fn data(self) -> Vec<Entry> {
        self.data
    }

    pub fn size_without_data() -> usize {
        8 + 4 + 2
    }

    pub fn from_bytes(mut raw_record: [u8; WAL_RECORD_SIZE]) -> anyhow::Result<Self> {
        let sequence_number = {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&raw_record[..8]);
            u64::from_le_bytes(buf)
        };

        let checksum_expected = {
            let mut buf = [0u8; 4];
            buf.copy_from_slice(&raw_record[8..12]);
            u32::from_le_bytes(buf)
        };

        let entry_count = {
            let mut buf = [0u8; 2];
            buf.copy_from_slice(&raw_record[12..14]);
            u16::from_le_bytes(buf)
        };

        let mut offset = 14;
        let data = (0..entry_count)
            .map(|_| {
                let (entry, consumed) = Entry::from_bytes(&raw_record[offset..])?;
                offset += consumed;
                Ok(entry)
            })
            .collect::<anyhow::Result<Vec<Entry>>>()?;

        let checksum = {
            raw_record[10..14].copy_from_slice(&WAL_CHECKSUM_MAGIC.to_le_bytes());
            let mut buf = [0u8; 4];
            let checksum_hash = blake3::hash(&raw_record[10..offset]);
            buf.copy_from_slice(&checksum_hash.as_bytes()[0..4]);
            u32::from_le_bytes(buf)
        };

        if checksum_expected != checksum {
            bail!("Wrong checksum")
        }

        Ok(Self {
            sequence_number,
            data,
        })
    }

    pub fn to_bytes(self) -> [u8; WAL_RECORD_SIZE] {
        let mut raw_record = [0u8; WAL_RECORD_SIZE];
        raw_record[0..8].copy_from_slice(&self.sequence_number.to_le_bytes());
        // skip checksum, fill later
        let entry_count: u16 = self
            .data
            .len()
            .try_into()
            .expect("Entries must fit in WAL_RECORD_SIZE");

        let mut offset = 14;
        for entry in self.data {
            let entry_len = entry.len();
            raw_record[offset..offset + entry_len].copy_from_slice(&entry.to_bytes());
            offset += entry_len;
        }

        let checksum: u32 = {
            raw_record[10..14].copy_from_slice(&WAL_CHECKSUM_MAGIC.to_le_bytes());
            let mut buf = [0u8; 4];
            let checksum_hash = blake3::hash(&raw_record[10..offset]);
            buf.copy_from_slice(&checksum_hash.as_bytes()[0..4]);
            u32::from_le_bytes(buf)
        };

        raw_record[8..12].copy_from_slice(&checksum.to_le_bytes());
        raw_record[12..14].copy_from_slice(&entry_count.to_le_bytes());
        raw_record
    }
}

#[cfg(test)]
pub mod tests {
    use super::Record;
    use crate::wal::entry::Entry;

    pub fn create_record(sequence_number: u64, range: std::ops::Range<u64>) -> Record {
        let make_entry = |i: u64| -> Entry {
            if i % 2 == 0 {
                Entry::Clear { bucket_index: i }
            } else {
                Entry::Update {
                    page_id: {
                        let mut page_id = [0; 16];
                        page_id[0..8].copy_from_slice(&i.to_le_bytes());
                        page_id
                    },
                    page_diff: {
                        let mut page_diff = [0; 16];
                        page_diff[8..16].copy_from_slice(&i.to_le_bytes());
                        page_diff
                    },
                    changed: (0..i.count_ones()).map(|i| [i as u8; 32]).collect(),
                    bucket_index: i,
                }
            }
        };

        Record {
            sequence_number,
            data: range.map(|i| make_entry(i)).collect(),
        }
    }

    #[test]
    fn record_code_and_decode() {
        let record = create_record(186, 100..400);

        let raw_record = record.clone().to_bytes();
        let decoded_record = Record::from_bytes(raw_record).unwrap();
        assert_eq!(decoded_record, record);
    }
}
