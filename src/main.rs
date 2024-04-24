use crossbeam::channel;
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicBool};
use rand::Rng;
use std::env;

#[macro_export]
macro_rules! debug_println {
    ($($arg:tt)*) => {
        if cfg!(debug_assertions) {
            println!($($arg)*);
        }
    }
}

fn main() {
    let producer_count: usize = env::args()
        .nth(2)
        .unwrap_or_else(|| String::from("5"))
        .parse()
        .expect("Failed to parse producer count");

    let consumer_count: usize = env::args()
        .nth(3)
        .unwrap_or_else(|| String::from("5"))
        .parse()
        .expect("Failed to parse consumer count");

    let seconds: u64 = env::args()
        .nth(1)
        .unwrap_or_else(|| String::from("5"))
        .parse()
        .expect("Failed to parse seconds");

    let multi = env::args().nth(4).is_some();

    for p in 1..producer_count+1 {
        print!("p{}: ", p);
        for c in 1..consumer_count+1 {
            let bandwidth;
            if (multi) {
                bandwidth = multi_mpmc(p, c, seconds);
            } else {
                bandwidth = single_mpmc(p, c, seconds);
            }

            print!("{} ", bandwidth);
        }
        println!(""); // newline
    }
}

fn producer(tx: channel::Sender<String>, id: usize, should_exit: Arc<AtomicBool>) {
    let mut rng = rand::thread_rng();
    let mut msg_count = 0;
    loop {
        if msg_count % 10 == 0 && should_exit.load(Ordering::Relaxed) {
            break;
        }
        let len: usize = rng.gen_range(100..1000);
        let msg = "a".repeat(len);
        // exit if send returns Err
        if let Err(_) = tx.send_timeout(msg, Duration::from_millis(10)) {
            break;
        }
        //debug_println!("Producer {} sent a message.", id);
        msg_count += 1;
    }
}

fn consumer(rx: channel::Receiver<String>, id: usize, counter: Arc<AtomicUsize>, length: Arc<AtomicUsize>, should_exit: Arc<AtomicBool>) {
    let mut local_count = 0;
    let mut total_length = 0;
    loop {
        if local_count % 10 == 0 && should_exit.load(Ordering::Relaxed) {
            break;
        }
        match rx.recv_timeout(Duration::from_millis(10)) {
            Ok(s) => {
                local_count += 1;
                total_length += s.len();
                //debug_println!("Consumer {} received a message.", id);
            },
            Err(channel::RecvTimeoutError::Timeout) => {
                if should_exit.load(Ordering::Relaxed) {
                    break;
                }
            },
            Err(_) => {
                break;
            }
        }
    }
    counter.fetch_add(local_count, Ordering::SeqCst);
    length.fetch_add(total_length, Ordering::SeqCst);
}

fn single_mpmc(producer_count: usize, consumer_count: usize, seconds: u64) -> usize {
    let (tx, rx) = channel::unbounded::<String>();

    let counter = Arc::new(AtomicUsize::new(0));
    let length = Arc::new(AtomicUsize::new(0));
    let should_exit: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

    let mut handles = vec![];

    for i in 0..producer_count {
        let tx = tx.clone();
        let should_exit = Arc::clone(&should_exit);
        let handle = thread::spawn(move || {
            producer(tx, i, should_exit);
        });
        handles.push(handle);
    }

    for i in 0..consumer_count {
        let rx = rx.clone();
        let counter = Arc::clone(&counter);
        let should_exit = Arc::clone(&should_exit);
        let length = Arc::clone(&length);
        let handle = thread::spawn(move || {
            consumer(rx, i, counter, length, should_exit);
        });
        handles.push(handle);
    }
    drop(tx);
    drop(rx);

    thread::sleep(Duration::from_secs(seconds));
    should_exit.store(true, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }
    
    counter.load(Ordering::Relaxed) / seconds as usize
}


fn multi_mpmc(producer_count: usize, consumer_count: usize, seconds: u64) -> usize {
    let counters: Vec<_> = (0..consumer_count).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let lengths: Vec<_> = (0..consumer_count).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let should_exit: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    let channels: Vec<_> = (0..consumer_count).map(|_| channel::unbounded::<String>()).collect();

    let round_robin_counter = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for i in 0..producer_count {
        let channels = channels.clone();
        let round_robin_counter = Arc::clone(&round_robin_counter);
        let should_exit = Arc::clone(&should_exit);
        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut msg_count = 0;
            loop {
                if msg_count % 10 == 0 && should_exit.load(Ordering::Relaxed) {
                    break;
                }

                let len: usize = rng.gen_range(100..1000);
                let msg = "a".repeat(len);
                let index = round_robin_counter.fetch_add(1, Ordering::SeqCst) % consumer_count;
                if let Err(_) = channels[index].0.send_timeout(msg, Duration::from_millis(10)) {
                    break;
                }
                msg_count += 1;
            }
        });
        handles.push(handle);
    }

    for i in 0..consumer_count {
        let (_tx, rx) = channels[i].clone();
        let counter = Arc::clone(&counters[i]);
        let length = Arc::clone(&lengths[i]);
        let should_exit = Arc::clone(&should_exit);
        let handle = thread::spawn(move || {
            consumer(rx.clone(), i, counter, length, should_exit);
        });
        handles.push(handle);
    }

    thread::sleep(Duration::from_secs(seconds));
    should_exit.store(true, Ordering::SeqCst);

    for handle in handles {
        handle.join().unwrap();
    }

    counters.iter().map(|counter| counter.load(Ordering::SeqCst)).sum()
}