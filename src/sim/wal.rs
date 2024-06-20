use super::PageDiff;

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

const WAL_FRAME_SIZE: usize = 256 * 1024;
// walawala
const WAL_CHECKSUM_MAGIC: u32 = 0x00a10a1a;