use std::io;

use rand::Rng;

use crate::core::KVDB;

mod core;

fn main() -> io::Result<()> {
    let mut db = KVDB::new("0.log")?;
    let mut rng = rand::thread_rng();
    let k = format!("user::{}", rng.gen_range(0..100));
    if let Some(value) = db.get(k.clone())? {
        println!("{} => {}", k, serde_json::from_str::<i32>(&value)?);
    } else {
        println!("key[{}] doesn't exist", k);
    }
    for _ in 0..50 {
        let k = format!("user::{}", rng.gen_range(0..100));
        if rng.gen::<f64>() < 0.2 {
            db.remove(k)?;
        } else {
            db.set(k, rng.gen::<i32>())?;
        }
    }
    Ok(())
}
