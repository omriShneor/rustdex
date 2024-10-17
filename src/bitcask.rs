use std::time::SystemTime;
use std::path::PathBuf;
use std::fs;
use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Read, Write};
use serde::{Serialize, Deserialize};
use bincode;
use std::io;
enum BitcaskResult {
    WriteOk(usize),
    EntryData(Vec<u8>),
    KeyNotFoundError,
    SerializeError(String),
    KeysList(Vec<String>),
    UnExpectedError(String),
    Ok()
}

#[derive(Serialize, Deserialize, Debug)]
struct DataFileEntry {
    //crc: i32,
    tstamp: SystemTime,
    ksz: usize,
    value_sz: usize,
    key: String,
    value: Vec<u8>
}

struct KeyDirEntry {
    file_id: usize,
    value_sz: usize,
    value_pos: usize,
    tstamp: SystemTime
}

struct KeyDir {
    map: HashMap<String, KeyDirEntry>
}

struct BitcaskHandler {
    dir: PathBuf,
    active_file: fs::File,
    key_dir: KeyDir 
}

impl BitcaskHandler {
    fn new(dir_path: &str) -> Self {
        let path = PathBuf::from(dir_path);
        
        if !path.exists() {
            fs::create_dir(&path)
                .unwrap_or_else(|e| panic!("Error creating directory: {}, {}", dir_path, e));
        }
        
        let active_file_path = path.join("active_file");
        let active_file: fs::File;
        if !active_file_path.exists() {
            active_file = fs::File::create(&active_file_path)
                .unwrap_or_else(|e| panic!("Error creating active file: {}, {}", active_file_path.display(), e));
        } else {
            active_file = fs::OpenOptions::new()
                .read(true)
                .append(true)
                .open(&active_file_path)
                .unwrap_or_else(|e| panic!("Error opening active file: {}, {}", active_file_path.display(), e));
        }
        
        Self { 
            dir: path, 
            key_dir: KeyDir { map: HashMap::new() },
            active_file: active_file
        }
    }
    fn _cleanup(&mut self) -> io::Result<()> {
        // Sync and flush the active file
        fs::remove_file(self.dir.join("active_file"))?;

        // Remove all files in the directory
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Err(e) = fs::remove_file(&path) {
                    eprintln!("Failed to remove file {}: {}", path.display(), e);
                }
            }
        }

        match fs::remove_dir(&self.dir) {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("Failed to remove directory {}: {}", self.dir.display(), e);
                Err(e)
            }
        }
    }
    fn get(&mut self, key: String) -> BitcaskResult {
        let key_dir_entry = self.key_dir.map.get(&key);
        if key_dir_entry.is_none() {
            return BitcaskResult::KeyNotFoundError;
        }
        let key_dir_entry = key_dir_entry.unwrap();
        let mut buffer = vec![0; key_dir_entry.value_sz];
        self.active_file.seek(SeekFrom::Start(key_dir_entry.value_pos as u64)).unwrap();
        self.active_file.read_exact(&mut buffer).unwrap();
        BitcaskResult::EntryData(buffer)
    }
    fn put(&mut self, key: String, value: Vec<u8>) -> BitcaskResult {
        let value_sz = value.len();
        let data_file_entry = DataFileEntry {
            tstamp: SystemTime::now(),
            ksz: key.len(),
            value_sz: value_sz,
            key: key.clone(),
            value: value
        };
        let value_pos: usize = match self.append_to_active_file(data_file_entry) {
            Ok(BitcaskResult::WriteOk(pos)) => pos,
            Err(e) => return e,
            _ => return BitcaskResult::UnExpectedError("Unexpected error".to_string())
        };
        self.key_dir.map.insert(key, KeyDirEntry {
            file_id: 0,
            value_sz: value_sz,
            value_pos: value_pos,
            tstamp: SystemTime::now()
        });
        BitcaskResult::Ok()
    }
    fn delete(&self, key: String) -> BitcaskResult {
        todo!();
    }
    fn list_keys(&self) -> BitcaskResult {
        todo!();
    }
    fn append_to_active_file(&mut self, data_file_entry: DataFileEntry) -> Result<BitcaskResult, BitcaskResult> {
        let serialized = bincode::serialize(&data_file_entry).map_err(|e| BitcaskResult::SerializeError(e.to_string()))?;
        self.active_file.write_all(&serialized).map_err(|e| BitcaskResult::SerializeError(e.to_string()))?;
        self.active_file.sync_data().map_err(|e| BitcaskResult::SerializeError(e.to_string()))?;
        Ok(BitcaskResult::WriteOk(self.active_file.metadata().unwrap().len() as usize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn setup_test_dir(dir_path: &str) {
        let path = PathBuf::from(dir_path);
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
    }

    #[test]
    fn test_new() {
        let dir_path = "test_dir";
        setup_test_dir(dir_path); // Ensure the directory is clean before the test

        let mut bitcask = BitcaskHandler::new(dir_path);

        // Test directory creation
        let dir_path = PathBuf::from(dir_path);
        assert!(dir_path.exists(), "Directory should be created");
        assert!(dir_path.is_dir(), "Created path should be a directory");

        // Test BitcaskHandler properties
        assert_eq!(bitcask.dir, dir_path, "BitcaskHandler should use the correct directory");
        assert!(bitcask.key_dir.map.is_empty(), "KeyDir should start empty");

        // Test active file creation
        assert!(bitcask.active_file.metadata().unwrap().is_file(), "Active file should exist after BitcaskHandler creation");
        bitcask._cleanup().unwrap();
        assert!(!dir_path.exists(), "Directory should be deleted");
    }

    #[test]
    fn test_put_once() {
        let dir_path = "test_dir";
        setup_test_dir(dir_path); // Ensure the directory is clean before the test

        let mut bitcask = BitcaskHandler::new(dir_path);

        let key = "test_key";
        let value = "test_value".as_bytes().to_vec();

        let result = bitcask.put(key.to_string(), value.clone());
        assert!(matches!(result, BitcaskResult::Ok()));

        let result = bitcask.get(key.to_string());
        assert!(matches!(result, BitcaskResult::EntryData(ref v) if *v == value), "Get operation should return the correct value");
        bitcask._cleanup().unwrap();
    }
}
