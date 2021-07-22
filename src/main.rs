use std::{
    io,
};

use rand::Rng;

use crate::core::KVDB;

mod core;

fn main() -> io::Result<()> {
    let mut db = KVDB::new()?;
    let mut rng = rand::thread_rng();
    for i in 0..20 {
        println!("第{}轮", i + 1);
        println!("随机写入10万条数据");
        for _ in 0..100000 {
            let k = format!("user::{}", rng.gen_range(0..1000));
            if rng.gen::<f64>() < 0.2 {
                db.remove(k)?;
            } else {
                db.set(k, rng.gen::<(i32, bool, f64)>())?;
            }
        }
        println!("随机获取100条数据");
        for _ in 0..100 {
            let k = format!("user::{}", rng.gen_range(0..1000));
            if let Some(value) = db.get(k.clone())? {
                println!("[{}] => {:?}", k, serde_json::from_str::<(i32, bool, f64)>(&value)?);
            } else {
                println!("key [{}] 不存在", k);
            }
        }
    }
    Ok(())
}
