use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set(String, String),
    Remove(String),
}

#[derive(Debug)]
struct CommandInfo {
    pos: u64,
    len: u64,
    log_idx: u64,
}

pub struct KVDB {
    writer: BufWriterWithPos<File>,
    /// 每个日志文件对应一个reader
    readers: HashMap<u64, BufReaderWithPos<File>>,
    index: HashMap<String, CommandInfo>,
    cur_log_idx: u64,
    log_size: u64,
}

impl KVDB {
    pub fn new() -> io::Result<Self> {
        let mut reader = HashMap::new();
        let mut logs = if !Path::new("logs/").is_dir() {
            fs::create_dir("logs")?;
            File::create("logs/0.log")?;
            reader.insert(
                0,
                BufReaderWithPos::new(OpenOptions::new().read(true).open("logs/0.log")?)?,
            );
            vec![0u64]
        } else {
            // 遍历logs目录下所有的log文件，创建对应每个日志文件的reader
            // 并返回日志序列数组
            let mut tmp = vec![];
            for entity in fs::read_dir("logs")? {
                let entity = entity.unwrap();
                if entity.metadata().unwrap().is_file() {
                    let file_idx = entity
                        .file_name()
                        .to_str()
                        .unwrap()
                        .split('.')
                        .collect::<Vec<_>>()[0]
                        .parse::<u64>()
                        .unwrap();

                    reader.insert(
                        file_idx,
                        BufReaderWithPos::new(
                            OpenOptions::new()
                                .read(true)
                                .open(build_log_path(file_idx))?,
                        )?,
                    );
                    tmp.push(file_idx);
                }
            }
            tmp
        };
        logs.sort();
        // 写入最新的日志中
        let cur_log_index = *logs.iter().max().unwrap();

        let mut db = KVDB {
            writer: BufWriterWithPos::new(
                OpenOptions::new()
                    .append(true)
                    .open(build_log_path(cur_log_index))?,
            )?,
            readers: reader,
            index: HashMap::new(),
            cur_log_idx: cur_log_index,
            log_size: 64 * 1024 * 1024, // 默认日志阈值，超过后压缩日志
        };
        println!("正在构建内存索引......");
        for log_idx in logs {
            db.load(log_idx)?;
        }
        println!("内存索引构建完成");
        Ok(db)
    }

    /// 根据日志内容构建索引
    fn load(&mut self, log_idx: u64) -> io::Result<()> {
        let reader = self.readers.get_mut(&log_idx).unwrap();
        let mut pos = reader.seek(SeekFrom::Start(0))?;

        let mut iter = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
        while let Some(cmd) = iter.next() {
            let new_pos = iter.byte_offset() as u64;
            match cmd? {
                Command::Set(k, _) => {
                    self.index.insert(
                        k,
                        CommandInfo {
                            pos,
                            len: new_pos - pos,
                            log_idx,
                        },
                    );
                }
                Command::Remove(k) => {
                    self.index.remove(&k);
                }
            }
            pos = new_pos;
        }
        Ok(())
    }

    pub fn set(&mut self, k: String, v: impl Serialize) -> io::Result<()> {
        let pos = self.writer.pos;
        let cmd = Command::Set(k.clone(), serde_json::to_string(&v)?);
        self.writer.writeln(cmd)?;
        self.writer.flush()?;
        self.index.insert(
            k,
            CommandInfo {
                pos,
                len: self.writer.pos - pos,
                log_idx: self.cur_log_idx,
            },
        );
        self.check_log_size()?;
        Ok(())
    }

    pub fn get(&mut self, k: String) -> io::Result<Option<String>> {
        match self.index.get(&k) {
            None => Ok(None),
            Some(cmd_info) => {
                let reader = self.readers.get_mut(&cmd_info.log_idx).unwrap();
                reader.seek(SeekFrom::Start(cmd_info.pos))?;
                if let Command::Set(_, v) = serde_json::from_reader(reader.take(cmd_info.len))? {
                    Ok(Some(v))
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Serde from_reader() failed",
                    ))
                }
            }
        }
    }

    pub fn remove(&mut self, k: String) -> io::Result<()> {
        let cmd = Command::Remove(k.clone());
        self.writer.writeln(cmd)?;
        self.writer.flush()?;
        self.index.remove(&k);
        self.check_log_size()?;
        Ok(())
    }

    /// 压缩日志文件，idx+1为压缩后的日志，idx+2为操作完成后的目标日志
    /// 保存新文件的reader，同时删除老的日志文件与对应reader
    fn compact(&mut self) -> io::Result<()> {
        let old_idx = self.cur_log_idx;
        self.cur_log_idx += 2;
        for i in 1..=2 {
            File::create(build_log_path(old_idx + i))?;
            self.readers.insert(
                old_idx + i,
                BufReaderWithPos::new(
                    OpenOptions::new()
                        .read(true)
                        .open(build_log_path(old_idx + i))?,
                )?,
            );
        }
        self.writer = BufWriterWithPos::new(
            OpenOptions::new()
                .append(true)
                .open(build_log_path(self.cur_log_idx))?,
        )?;

        let mut writer = BufWriterWithPos::new(
            OpenOptions::new()
                .append(true)
                .open(build_log_path(old_idx + 1))?,
        )?;
        for cmd_info in self.index.values_mut() {
            let reader = self.readers.get_mut(&cmd_info.log_idx).unwrap();
            reader.seek(SeekFrom::Start(cmd_info.pos))?;
            io::copy(&mut reader.take(cmd_info.len), &mut writer)?;
            *cmd_info = CommandInfo {
                pos: writer.pos - cmd_info.len,
                len: cmd_info.len,
                log_idx: old_idx + 1,
            };
        }
        writer.flush()?;
        let stale_idxs = self
            .readers
            .keys()
            .filter(|idx| **idx <= old_idx)
            .cloned()
            .collect::<Vec<_>>();
        for idx in stale_idxs {
            self.readers.remove(&idx);
            fs::remove_file(build_log_path(idx))?;
        }
        Ok(())
    }

    fn check_log_size(&mut self) -> io::Result<()> {
        if self.writer.pos >= self.log_size {
            println!("开始压缩 {}.log", self.cur_log_idx);
            self.compact()?;
            println!("压缩完成并写入 {}.log", self.cur_log_idx - 1);
        }
        Ok(())
    }
}

fn build_log_path(idx: u64) -> String {
    format!("logs/{}.log", idx)
}

#[derive(Debug)]
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> io::Result<Self> {
        Ok(BufWriterWithPos {
            pos: inner.seek(SeekFrom::End(0))?,
            writer: BufWriter::new(inner),
        })
    }

    fn writeln(&mut self, value: impl Serialize) -> io::Result<usize> {
        let mut tmp = serde_json::to_vec(&value)?;
        tmp.push(b'\n');
        self.write(&tmp)
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

struct BufReaderWithPos<W: Read + Seek> {
    reader: BufReader<W>,
    pos: u64,
}

impl<W: Read + Seek> BufReaderWithPos<W> {
    fn new(mut inner: W) -> io::Result<Self> {
        Ok(BufReaderWithPos {
            pos: inner.seek(SeekFrom::Current(0))?,
            reader: BufReader::new(inner),
        })
    }
}

impl<W: Read + Seek> Read for BufReaderWithPos<W> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<W: Read + Seek> Seek for BufReaderWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}
