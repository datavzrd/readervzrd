use serde_json::{Deserializer, Value};
use std::fs::File;
use std::io::{self, BufReader};
use thiserror::Error;

enum FileFormat {
    Csv(char),
    Json,
}

impl FileFormat {
    pub fn from_file(file_path: &str, delimiter: Option<char>) -> Result<FileFormat, FileError> {
        match (std::path::Path::new(file_path)
            .extension()
            .unwrap()
            .to_str(), delimiter)
        {
            (Some("csv" | "tsv"), Some(d)) => Ok(FileFormat::Csv(d)),
            (Some("json"), _) => Ok(FileFormat::Json),
            _ => Err(FileError::UnknownFileFormat),
        }
    }
}

pub struct FileReader {
    file_format: FileFormat,
    file: BufReader<File>,
}

impl FileReader {
    pub fn new(file_path: &str, delimiter: Option<char>) -> Result<FileReader, FileError> {
        let file_format = FileFormat::from_file(file_path, delimiter)?;
        let file = BufReader::new(File::open(file_path)?);
        Ok(FileReader {
            file_format,
            file,
        })
    }

    pub fn headers(&mut self) -> Result<Vec<String>, FileError> {
        match &self.file_format {
            FileFormat::Csv(delimiter) => self.read_csv_headers(&delimiter.to_owned()),
            FileFormat::Json => self.read_json_headers(),
        }
    }

    fn read_csv_headers(&mut self, delimiter: &char) -> Result<Vec<String>, FileError> {
        let mut reader = csv::ReaderBuilder::new().delimiter(delimiter.to_owned() as u8).from_reader(&mut self.file);
        Ok(reader
            .headers()
            .unwrap()
            .iter()
            .map(|s| s.to_string())
            .collect())
    }

    fn read_json_headers(&mut self) -> Result<Vec<String>, FileError> {
        let mut headers = Vec::new();
        // Parse the JSON array at the outermost level
        if let Ok(value) = serde_json::from_reader(&mut self.file) {
            if let serde_json::Value::Array(array) = value {
                // Iterate over the array and collect headers from each object
                for item in array {
                    if let serde_json::Value::Object(obj) = item {
                        flatten_json_object(&mut headers, &obj, String::new());
                    }
                }
            }
        }
        Ok(headers)
    }

    pub fn records(&mut self) -> Result<FlexRecordIter, FileError> {
        match &self.file_format {
            FileFormat::Csv(delimiter) => Ok(FlexRecordIter::Csv(Box::new(self.read_csv_records(&delimiter.to_owned())))),
            FileFormat::Json => Ok(FlexRecordIter::Json(Box::new(self.read_json_records()?))),
        }
    }


    fn read_csv_records<'a>(&'a mut self, delimiter: &char) -> impl Iterator<Item = Vec<String>> + 'a {
        let reader = csv::ReaderBuilder::new().delimiter(delimiter.to_owned() as u8).from_reader(&mut self.file);
        reader.into_records().filter_map(Result::ok).map(|record| {
            record.iter().map(|field| field.to_string()).collect()
        })
    }

    pub fn read_json_records<'a>(&'a mut self) -> Result<impl Iterator<Item = Vec<String>> + 'a, FileError> {
        let deserializer = Deserializer::from_reader(&mut self.file).into_iter::<Value>();
        let iter = deserializer
            .filter_map(Result::ok)
            .flat_map(|value| {
                match value {
                    Value::Array(arr) => arr.into_iter().map(flatten_json_record),
                    _ => panic!("Expected JSON array"),
                }
            });
        Ok(iter)
    }
}

pub enum FlexRecordIter<'a> {
    Csv(Box<dyn Iterator<Item = Vec<String>> + 'a>),
    Json(Box<dyn Iterator<Item = Vec<String>> + 'a>),
}

impl<'a> Iterator for FlexRecordIter<'a> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            FlexRecordIter::Csv(iter) => iter.next(),
            FlexRecordIter::Json(iter) => iter.next(),
        }
    }
}

fn flatten_json_record(value: Value) -> Vec<String> {
    match value {
        Value::String(s) => vec![s],
        Value::Number(n) => vec![n.to_string()],
        Value::Array(a) => vec![serde_json::to_string(&a).unwrap()],
        Value::Object(obj) => obj
            .into_iter()
            .flat_map(|(_, v)| flatten_json_record(v))
            .collect(),
        _ => unreachable!("Unexpected value type"),
    }
}

fn flatten_json_object(
    headers: &mut Vec<String>,
    obj: &serde_json::Map<String, Value>,
    prefix: String,
) {
    for (key, value) in obj {
        match value {
            Value::Object(inner_obj) => {
                let new_prefix = if prefix.is_empty() {
                    key.to_string()
                } else {
                    format!("{}.{}", prefix, key)
                };
                flatten_json_object(headers, inner_obj, new_prefix);
            }
            _ => {
                let header = if prefix.is_empty() {
                    key.to_string()
                } else {
                    format!("{}.{}", prefix, key)
                };
                if !headers.contains(&header) {
                    headers.push(header);
                }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum FileError {
    #[error("Unknown file format")]
    UnknownFileFormat,
    #[error("Invalid JSON structure")]
    InvalidJsonStructure,
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
}

impl PartialEq for FileError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FileError::UnknownFileFormat, FileError::UnknownFileFormat) => true,
            (FileError::InvalidJsonStructure, FileError::InvalidJsonStructure) => true,
            (FileError::IoError(e1), FileError::IoError(e2)) => e1.kind() == e2.kind(),
            (_, _) => false,
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_headers() {
        let mut reader = FileReader::new("tests/test.csv", Some(',')).expect("Failed to create FileReader");
        let headers = reader.headers().expect("Failed to get headers");
        assert_eq!(headers, vec!["Name", "Age", "Country"]);
    }

    #[test]
    fn test_json_headers() {
        let mut reader = FileReader::new("tests/test.json", None).expect("Failed to create FileReader");
        let headers = reader.headers().expect("Failed to get headers");
        assert_eq!(headers, vec!["age", "country", "name"]);
    }

    #[test]
    fn test_nested_json_headers() {
        let mut reader =
            FileReader::new("tests/nested_test.json", Some(',')).expect("Failed to create FileReader");
        let headers = reader.headers().expect("Failed to get headers");
        assert_eq!(
            headers,
            vec!["age", "bank.account", "bank.institution", "country", "name"]
        );
    }

    #[test]
    fn test_csv_records() {
        let mut reader = FileReader::new("tests/test.csv", Some(',')).expect("Failed to create FileReader");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], vec!["John", "30", "USA"]);
        assert_eq!(records[1], vec!["Alice", "25", "UK"]);
        assert_eq!(records[2], vec!["Bob", "40", "Canada"]);
    }

    #[test]
    fn test_json_records() {
        let mut reader = FileReader::new("tests/test.json", None).expect("Failed to create FileReader");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], vec!["30", "USA", "John"]);
        assert_eq!(records[1], vec!["25", "UK", "Alice"]);
        assert_eq!(records[2], vec!["40", "Canada", "Bob"]);
    }

    #[test]
    fn test_nested_json_records() {
        let mut reader =
            FileReader::new("tests/nested_test.json", None).expect("Failed to create FileReader");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], vec!["30", "123456", "Chase", "USA", "John"]);
        assert_eq!(records[1], vec!["25", "654321", "Barclays", "UK", "Alice"]);
        assert_eq!(records[2], vec!["40", "789456", "TD", "Canada", "Bob"]);
    }

    #[test]
    fn test_tsv_records() {
        let mut reader = FileReader::new("tests/test.tsv", Some('\t')).expect("Failed to create FileReader");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], vec!["John", "30", "USA"]);
        assert_eq!(records[1], vec!["Alice", "25", "UK"]);
        assert_eq!(records[2], vec!["Bob", "40", "Canada"]);
    }

    #[test]
    fn test_unknown_file_format() {
        let result = FileReader::new("tests/test.txt", None);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), FileError::UnknownFileFormat);
    }

    #[test]
    fn test_json_records_with_inner_array() {
        let mut reader = FileReader::new("tests/inner_array_test.json", None).expect("Failed to create FileReader");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], vec!["30", "USA", "John", "[\"dog\",\"cat\"]"]);
        assert_eq!(records[1], vec!["25", "UK", "Alice", "[\"rabbit\"]"]);
        assert_eq!(records[2], vec!["40", "Canada", "Bob", "[]"]);
    }

    #[test]
    fn test_json_headers_with_inner_array() {
        let mut reader = FileReader::new("tests/inner_array_test.json", None).expect("Failed to create FileReader");
        let headers = reader.headers().expect("Failed to get headers");
        assert_eq!(headers, vec!["age", "country", "name", "pets"]);
    }
}
