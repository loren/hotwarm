#[macro_use]
extern crate log;

use env_logger::Env;
use indicatif::ProgressIterator;
use rand::prelude::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;
use redb::{Database, ReadableTable, TableDefinition};
use std::collections::BinaryHeap;
use std::env;
use std::time::Instant;

const NUM_ENTRIES: usize = 100_000;
const DATA_SIZE: usize = 3_000_000;
const NUM_RUNS: usize = 100;
const HOT_SIZE: usize = NUM_ENTRIES / 5; // a random 20% of the keys are "hot"
const WARM_TABLE: TableDefinition<&str, &str> = TableDefinition::new("warm");
const HOT_TABLE: TableDefinition<&str, &str> = TableDefinition::new("hot");

fn get_key(n: usize) -> String {
    format!("key {n}")
}

fn get_data<R: Rng>(rng: &mut R) -> String {
    (0..DATA_SIZE).map(|_| rng.gen_range('a'..='z')).collect()
}
fn main() {
    let env = Env::default()
        .filter_or("HW_LOG_LEVEL", "trace")
        .write_style_or("HW_LOG_STYLE", "always");

    env_logger::init_from_env(env);

    let args: Vec<String> = env::args().collect();
    // build two DB tables of key value entries...everything starts out in warm table
    let db = Database::create("hotwarmdb.redb").unwrap();
    let keys: Vec<String> = (0..NUM_ENTRIES).map(get_key).collect();
    if args.len() > 1 && args[1] == "setup" {
        info!(
            "Setting up DB with {} entries of {} bytes",
            NUM_ENTRIES, DATA_SIZE
        );
        setup(&db, &keys);
        return;
    }
    let mut rng = XorShiftRng::from_seed(Default::default());
    let hot_keys: Vec<String> = (0..HOT_SIZE)
        .map(|_| keys.choose(&mut rng).unwrap().to_string())
        .collect();

    // these hash maps track accesses per key
    let mut hot_access: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut warm_access: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    // need to initialize hot_access and warm_access from keys in DB
    for key in &keys {
        warm_access.insert(key.to_string(), 0);
    }

    // keep a running tally of bytes read from the DB
    let mut bytes_read: usize = 0;

    // Monitor the time it takes for each of the NUM_RUNS runs to complete
    for run_num in 0..NUM_RUNS {
        info!("Starting run {}", run_num);
        let start = Instant::now();
        for _ in 0..NUM_ENTRIES {
            // 80% of the time, pick a key from the hot keys
            let key = if rng.gen_range(0..100) < 80 {
                hot_keys.choose(&mut rng).unwrap().to_string()
            } else {
                keys.choose(&mut rng).unwrap().to_string()
            };
            let hot_entry = hot_access.entry(key.clone()).and_modify(|e| *e += 1);
            match hot_entry {
                std::collections::hash_map::Entry::Occupied(_) => {
                    // get from hot table
                    let read_txn = db.begin_read().unwrap();
                    let hot_db_table = read_txn.open_table(HOT_TABLE).unwrap();
                    let guard = hot_db_table.get(&key).unwrap().unwrap();
                    let val = guard.value();
                    bytes_read += val.len();
                }
                std::collections::hash_map::Entry::Vacant(_) => {
                    warm_access
                        .entry(key.clone())
                        .and_modify(|e| *e += 1)
                        .or_insert(1);
                    // get from warm table
                    let read_txn = db.begin_read().unwrap();
                    let warm_db_table = read_txn.open_table(WARM_TABLE).unwrap();
                    let guard = warm_db_table.get(&key).unwrap().unwrap();
                    let val = guard.value();
                    bytes_read += val.len();
                }
            }
        }
        let duration = start.elapsed();
        info!("Time elapsed for {} gets is: {:?}", NUM_ENTRIES, duration);

        // after each run, move popular keys from warm table to hot table
        let mut warm_heap = BinaryHeap::<(usize, String)>::new();
        for (key, access_count) in &warm_access {
            warm_heap.push((*access_count, key.to_string()));
        }
        let mut hot_heap = BinaryHeap::<(isize, String)>::new();
        for (key, access_count) in &hot_access {
            hot_heap.push((-(*access_count as isize), key.to_string()));
        }
        while needs_promoting(&warm_heap, &hot_heap, hot_access.len()) {
            let (access_count, key) = warm_heap.pop().unwrap();
            debug!(
                "Promoting {} with val {} from warm to hot",
                key, access_count
            );
            warm_access.remove(&key);
            hot_access.insert(key.clone(), access_count);
            let write_txn = db.begin_write().unwrap();
            {
                let mut warm_table = write_txn.open_table(WARM_TABLE).unwrap();
                let mut hot_table = write_txn.open_table(HOT_TABLE).unwrap();

                hot_table
                    .insert(&key, warm_table.remove(&key).unwrap().unwrap().value())
                    .unwrap();
            }
            write_txn.commit().unwrap();
        }
        // trim hot table to HOT_SIZE by demoting least popular keys to warm table
        while hot_access.len() > HOT_SIZE {
            let (access_count, key) = hot_heap.pop().unwrap();
            debug!(
                "Demoting {} with val {} from hot to warm",
                key,
                access_count.abs()
            );
            hot_access.remove(&key);
            warm_access.insert(key.clone(), -access_count as usize);
            let write_txn = db.begin_write().unwrap();
            {
                let mut warm_table = write_txn.open_table(WARM_TABLE).unwrap();
                let mut hot_table = write_txn.open_table(HOT_TABLE).unwrap();

                warm_table
                    .insert(&key, hot_table.remove(&key).unwrap().unwrap().value())
                    .unwrap();
            }
            write_txn.commit().unwrap();
        }
    }

    debug!("Hot table access stats: {:?}", hot_access.len());
    debug!("Warm table access stats: {:?}", warm_access.len());
    let read_txn = db.begin_read().unwrap();
    let warm_db_table = read_txn.open_table(WARM_TABLE).unwrap();
    let hot_db_table = read_txn.open_table(HOT_TABLE).unwrap();
    assert_eq!(hot_access.len(), hot_db_table.len().unwrap());
    assert_eq!(warm_access.len(), warm_db_table.len().unwrap());
    info!("Bytes read from DB: {}", bytes_read);
}

fn setup(db: &Database, keys: &Vec<String>) {
    let mut rng = XorShiftRng::from_seed(Default::default());
    // seed warm table with keys and random data
    let write_txn = db.begin_write().unwrap();
    {
        let mut warm_db_table = write_txn.open_table(WARM_TABLE).unwrap();

        for key in keys.iter().progress() {
            warm_db_table.insert(key, &get_data(&mut rng)).unwrap();
        }
    }
    write_txn.commit().unwrap();
    info!(
        "Warm table seeded with {} keys containing random {} char values",
        NUM_ENTRIES, DATA_SIZE
    );
}

// returns true if the warm table has a key that should be promoted to the hot table based on
// having an access count that is 2X the access count of the least popular key in the hot table.
fn needs_promoting(
    warm_heap: &BinaryHeap<(usize, String)>,
    hot_heap: &BinaryHeap<(isize, String)>,
    hot_size: usize,
) -> bool {
    let warm_freq = warm_heap.peek().unwrap().0;
    if hot_size < HOT_SIZE && warm_freq > 0 {
        return true;
    }
    if hot_heap.is_empty() {
        return false;
    }
    let hot_freq = hot_heap.peek().unwrap().0.abs() as usize;
    warm_freq > 2 * hot_freq
}
