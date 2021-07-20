use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set(String, String),
    Remove(String),
}

struct CommandInfo {
    pos: u64,
    len: u64,
}

pub struct KVDB {
    writer: BufWriterWithPos<File>,
    reader: BufReaderWithPos<File>,
    index: HashMap<String, CommandInfo>,
}

impl KVDB {
    pub fn new(path: &str) -> io::Result<Self> {
        if !Path::new(path).exists() {
            File::create(path)?;
        }
        Ok(KVDB {
            writer: BufWriterWithPos::new(OpenOptions::new().append(true).open(path)?)?,
            reader: BufReaderWithPos::new(OpenOptions::new().read(true).open(path)?)?,
            index: HashMap::new(),
        })
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
                len: self.writer.pos - pos - 1,
            },
        );
        Ok(())
    }

    pub fn get(&mut self, k: String) -> io::Result<Option<String>> {
        match self.index.get(&k) {
            None => Ok(None),
            Some(cmd_info) => {
                let reader = &mut self.reader;
                reader.seek(SeekFrom::Start(cmd_info.pos))?;
                let cmd_reader = reader.take(cmd_info.len);
                if let Command::Set(_, v) = serde_json::from_reader(cmd_reader)? {
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
        self.index.remove(&k).unwrap();
        Ok(())
    }
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
