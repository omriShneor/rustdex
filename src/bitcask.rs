use std::time::SystemTime;
use std::path::{Path, PathBuf};
use std::fs;
use std::collections::HashMap;

enum BitcaskResult {
    Ok(Vec<u8>),
    NotFoundError,
    Err(String),
    KeysList(Vec<String>),
}

struct DataFileEntry {
    crc: i32,
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
        if !active_file_path.exists() {
            fs::File::create(&active_file_path)
                .unwrap_or_else(|e| panic!("Error creating active file: {}, {}", active_file_path.display(), e));
        } else {
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&active_file_path)
                .unwrap_or_else(|e| panic!("Error opening active file: {}, {}", active_file_path.display(), e));
        }
        
        Self { 
            dir: path, 
            key_dir: KeyDir { map: HashMap::new() } 
        }
    }
    fn get(&self, key: String) -> BitcaskResult {
        todo!();
    }
    fn put(&self, key: String, value: Vec<u8>) -> BitcaskResult {
        todo!();
    }
    fn delete(&self, key: String) -> BitcaskResult {
        todo!();
    }
    fn list_keys(&self) -> BitcaskResult {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_new() {
        let dir_path = "test_dir";
        let bitcask = BitcaskHandler::new(dir_path);

        // Test directory creation
        let dir_path = PathBuf::from(dir_path);
        assert!(dir_path.exists(), "Directory should be created");
        assert!(dir_path.is_dir(), "Created path should be a directory");

        // Test BitcaskHandler properties
        assert_eq!(bitcask.dir, dir_path, "BitcaskHandler should use the correct directory");
        assert!(bitcask.key_dir.map.is_empty(), "KeyDir should start empty");

        // Test active file creation
        let active_file_path = dir_path.join("active_file");
        assert!(active_file_path.exists(), "Active file should exist after BitcaskHandler creation");
        assert!(active_file_path.is_file(), "Active file should be a file");

        // Delete the test directory and active file
        fs::remove_file(active_file_path).unwrap();
        fs::remove_dir(dir_path).unwrap();
    }
}