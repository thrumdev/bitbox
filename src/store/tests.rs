use crate::store::{
    io::{start_io_worker, CompleteIo, IoCommand, PageIndex},
    *,
};
use crossbeam_channel::{Receiver, Sender};
use std::io::{Read, Seek};

fn write_pages(
    tx: &Sender<IoCommand>,
    rx: &Receiver<CompleteIo>,
    num_pages: u64,
) -> Vec<(PageIndex, Page)> {
    let pages: Vec<_> = (0..num_pages / 2)
        .map(|i| {
            let j = dbg!((2 * i as u64) + 1);
            (PageIndex::Data(j), [j as u8; PAGE_SIZE])
        })
        .collect();

    for (page_index, page) in pages.iter() {
        let _ = tx
            .send(IoCommand {
                kind: io::IoKind::Write,
                handle: 0,
                page_id: page_index.clone(),
                buf: Box::new(*page),
            })
            .unwrap();
    }
    for _ in 0..pages.len() {
        // make sure every write is correctly performed
        assert!(rx.recv().unwrap().result.is_ok());
    }

    pages
}

#[test]
fn multiple_writes() {
    let file = "multiple_writes";
    let num_pages = 100;

    // create the store file
    create(file.into(), num_pages).expect("Error creating the store file");
    // open the store file
    let (store, _meta_bytes) = Store::open(file.into()).expect("Error opening the store file");

    let written: Vec<_> = {
        let store = std::sync::Arc::new(store);

        // start io_uring worker
        let (tx, rxs) = start_io_worker(store.clone(), 1);
        let rx = &rxs[0];

        write_pages(&tx, rx, num_pages as u64)
            .into_iter()
            .map(|p| (p.0.index_in_store(&store) * PAGE_SIZE as u64, p.1))
            .collect()
    };

    let mut store_file = OpenOptions::new()
        .read(true)
        .open(file)
        .expect("Error opening the store file");
    let mut page = [0u8; PAGE_SIZE];

    for (page_index, expected_page) in written {
        store_file
            .seek(std::io::SeekFrom::Start(page_index))
            .expect("Error seeking store file");
        let _ = store_file
            .read(&mut page)
            .expect("Error reading store file");
        assert_eq!(page, expected_page)
    }

    std::fs::remove_file(file).unwrap();
}

#[test]
fn multiple_reads() {
    let file = "multiple_reads";
    let num_pages = 100;

    // create the store file
    create(file.into(), num_pages).expect("Error creating the store file");
    // open the store file
    let (store, _meta_bytes) = Store::open(file.into()).expect("Error opening the store file");

    let store = std::sync::Arc::new(store);

    // start io_uring worker
    let (tx, rxs) = start_io_worker(store.clone(), 1);
    let rx = &rxs[0];

    let written = write_pages(&tx, rx, num_pages as u64);

    // send all page reads
    for (page_index, _page) in written.iter() {
        let _ = tx
            .send(IoCommand {
                kind: io::IoKind::Read,
                handle: 0,
                page_id: page_index.clone(),
                buf: Box::new([0u8; PAGE_SIZE]),
            })
            .unwrap();
    }

    for _ in 0..written.len() {
        let io = rx.recv().unwrap();
        assert!(io.result.is_ok());
        let IoCommand {
            kind: io::IoKind::Read,
            handle: 0,
            page_id,
            buf,
        } = io.command
        else {
            panic!("Unexpected command kind or handle")
        };
        let expected_page = written
            .iter()
            .find(|(id, _page)| *id == page_id)
            .expect("Read page must be present")
            .1;
        assert_eq!(*buf, expected_page);
    }

    std::fs::remove_file(file).unwrap();
}
