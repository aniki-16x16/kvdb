use serde::{Deserialize, Serialize};
use std::io;

use crate::core::KVDB;

mod core;

fn main() -> io::Result<()> {
    let mut db = KVDB::new("0.log")?;
    db.set(String::from("kvdb"), true)?;
    db.set(String::from("author"), "Aniki")?;
    db.set(String::from("version"), 0)?;
    db.set(String::from("version"), 1)?;
    db.set(
        String::from("user-1"),
        Person {
            name: String::from("Loren Lausi"),
            age: 15,
        },
    )?;
    db.set(
        String::from("user-2"),
        Person {
            name: String::from("Daiv Oiqud"),
            age: 99,
        },
    )?;
    db.set(
        String::from("user-1"),
        Person {
            name: String::from("Sadkuren Zoack"),
            age: 1,
        },
    )?;
    db.remove(String::from("user-1"))?;
    println!("{:#?}", db.get(String::from("user-1"))?);
    println!(
        "{:#?}",
        serde_json::from_str::<Person>(&db.get(String::from("user-2"))?.unwrap())?
    );
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Person {
    name: String,
    age: usize,
}
