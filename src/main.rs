use crossbeam::channel;
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use rand::Rng;

fn producer(tx: channel::Sender<String>, id: i32) {
    let mut rng = rand::thread_rng();
    loop {
        let len: usize = rng.gen_range(1..100);
        let msg = "a".repeat(len);
        tx.send(msg).unwrap();
        println!("Producer {} sent a message.", id);
    }
}

fn consumer(rx: channel::Receiver<String>, id: i32, counter: Arc<AtomicUsize>) {
    let mut local_count = 0;
    loop {
        match rx.recv() {
            Ok(_) => {
                local_count += 1;
                println!("Consumer {} received a message.", id);
            },
            Err(_) => {
                println!("Error receiving from channel or channel has been closed.");
                break;
            }
        }
    }
    counter.fetch_add(local_count, Ordering::SeqCst);
}

fn main() {
    let (tx, rx) = channel::unbounded();

    let producer_count = 5;
    let consumer_count = 5;

    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for i in 0..producer_count {
        let tx = tx.clone();
        let handle = thread::spawn(move || {
            producer(tx, i);
        });
        handles.push(handle);
    }

    for i in 0..consumer_count {
        let rx = rx.clone();
        let counter = Arc::clone(&counter);
        let handle = thread::spawn(move || {
            consumer(rx, i, counter);
        });
        handles.push(handle);
    }

    let tx_timer = tx.clone();
    let timer_handle = thread::spawn(move || {
        thread::sleep(Duration::from_secs(30));
        drop(tx_timer);
    });

    for handle in handles {
        handle.join().unwrap();
    }
    timer_handle.join().unwrap();

    println!("Messages processed: {}", counter.load(Ordering::SeqCst));
}