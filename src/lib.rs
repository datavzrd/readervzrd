use parquet::data_type::AsBytes;
use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader as ParquetFileReader, SerializedFileReader};
use parquet::record::reader::RowIter;
use serde_json::{Deserializer, Value};
use std::fs::File;
use std::io::{self, BufReader, Seek, SeekFrom};
use std::sync::Arc;
use thiserror::Error;

enum FileFormat {
    Csv(char),
    Json,
    Parquet,
}

impl FileFormat {
    pub fn from_file(file_path: &str, delimiter: Option<char>) -> Result<FileFormat, FileError> {
        let extension = std::path::Path::new(file_path)
            .extension()
            .ok_or(FileError::MissingExtension(file_path.to_string()))?;
        match (extension.to_str(), delimiter) {
            (Some("csv" | "tsv"), Some(d)) => Ok(FileFormat::Csv(d)),
            (Some("json"), _) => Ok(FileFormat::Json),
            (Some("parquet"), _) => Ok(FileFormat::Parquet),
            _ => Err(FileError::UnknownFileFormat),
        }
    }
}

/// A struct that reads records from a file.
/// The file can be in CSV, JSON or Parquet format.
/// The delimiter for CSV files can be specified.
///
/// # Examples
///
/// ```
/// use readervzrd::FileReader;
///
/// let mut reader = FileReader::new("tests/test.csv", Some(',')).expect("Failed to create FileReader");
/// let headers = reader.headers().expect("Failed to get headers");
/// let records: Vec<Vec<String>> = reader.records().unwrap().collect();
/// ```
///
/// ```
/// use readervzrd::FileReader;
///
/// let mut reader = FileReader::new("tests/test.json", None).expect("Failed to create FileReader");
/// let headers = reader.headers().expect("Failed to get headers");
/// let records: Vec<Vec<String>> = reader.records().unwrap().collect();
/// ```
///
/// ```
/// use readervzrd::FileReader;
///
/// let mut reader = FileReader::new("tests/test.parquet", None).expect("Failed to create FileReader");
/// let headers = reader.headers().expect("Failed to get headers");
/// let records: Vec<Vec<String>> = reader.records().unwrap().collect();
/// ```
pub struct FileReader {
    file_format: FileFormat,
    file: BufReader<File>,
}

impl FileReader {
    /// Creates a new FileReader instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use readervzrd::FileReader;
    ///
    /// let mut reader = FileReader::new("tests/test.csv", Some(',')).expect("Failed to create FileReader"); // Create a new FileReader instance for CSV file
    /// ```
    ///
    /// ```
    /// use readervzrd::FileReader;
    ///
    /// let mut reader = FileReader::new("tests/test.parquet", None).expect("Failed to create FileReader"); // Create a new FileReader instance for Parquet file
    /// ```
    /// ```
    /// use readervzrd::FileReader;
    ///
    /// let mut reader = FileReader::new("tests/test.json", None).expect("Failed to create FileReader"); // Create a new FileReader instance for JSON file
    /// ```
    pub fn new(file_path: &str, delimiter: Option<char>) -> Result<FileReader, FileError> {
        let file_format = FileFormat::from_file(file_path, delimiter)?;
        let file = BufReader::new(File::open(file_path)?);
        Ok(FileReader { file_format, file })
    }

    /// Returns the headers of the file.
    ///
    /// # Examples
    ///
    /// ```
    /// use readervzrd::FileReader;
    ///
    /// let mut reader = FileReader::new("tests/test.csv", Some(',')).expect("Failed to create FileReader");
    /// let headers = reader.headers().expect("Failed to get headers");
    /// ```
    pub fn headers(&mut self) -> Result<Vec<String>, FileError> {
        match &self.file_format {
            FileFormat::Csv(delimiter) => self.read_csv_headers(&delimiter.to_owned()),
            FileFormat::Json => self.read_json_headers(),
            FileFormat::Parquet => self.read_parquet_headers(),
        }
    }

    fn read_csv_headers(&mut self, delimiter: &char) -> Result<Vec<String>, FileError> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(*delimiter as u8)
            .from_reader(&mut self.file);
        let headers = reader
            .headers()
            .unwrap()
            .iter()
            .map(|s| s.to_string())
            .collect();
        self.file.seek(SeekFrom::Start(0))?;
        Ok(headers)
    }

    fn read_json_headers(&mut self) -> Result<Vec<String>, FileError> {
        let mut headers = Vec::new();
        if let Ok(serde_json::Value::Array(array)) = serde_json::from_reader(&mut self.file) {
            for item in array {
                if let serde_json::Value::Object(obj) = item {
                    flatten_json_object(&mut headers, &obj, String::new());
                }
            }
        }
        Ok(headers)
    }

    fn read_parquet_headers(&mut self) -> Result<Vec<String>, FileError> {
        // Reset file position to start
        self.file.seek(SeekFrom::Start(0))?;

        // Create a parquet file reader
        let file_reader = SerializedFileReader::new(self.file.get_ref().try_clone()?)?;
        let parquet_metadata = file_reader.metadata();
        let schema = parquet_metadata.file_metadata().schema_descr();

        // Extract column names from schema
        let headers: Vec<String> = schema
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        Ok(headers)
    }

    /// Returns an iterator over the records of the file.
    /// Each record is a vector of strings.
    ///
    /// # Examples
    ///
    /// ```
    /// use readervzrd::FileReader;
    ///
    /// let mut reader = FileReader::new("tests/test.csv", Some(',')).expect("Failed to create FileReader");
    /// for record in reader.records().unwrap() {
    ///    println!("{:?}", record);
    /// }
    /// ```
    pub fn records(&mut self) -> Result<FlexRecordIter, FileError> {
        match &self.file_format {
            FileFormat::Csv(delimiter) => Ok(FlexRecordIter::Csv(Box::new(
                self.read_csv_records(&delimiter.to_owned()),
            ))),
            FileFormat::Json => Ok(FlexRecordIter::Json(Box::new(self.read_json_records()?))),
            FileFormat::Parquet => Ok(FlexRecordIter::Parquet(Box::new(
                self.read_parquet_records()?,
            ))),
        }
    }

    fn read_csv_records<'a>(
        &'a mut self,
        delimiter: &char,
    ) -> impl Iterator<Item = Vec<String>> + 'a {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(*delimiter as u8)
            .from_reader(&mut self.file);
        let records: Vec<Vec<String>> = reader
            .records()
            .filter_map(Result::ok)
            .map(|record| record.iter().map(|field| field.to_string()).collect())
            .collect();
        self.file
            .seek(SeekFrom::Start(0))
            .expect("Failed to seek to start");
        records.into_iter()
    }

    pub fn read_json_records(
        &mut self,
    ) -> Result<impl Iterator<Item = Vec<String>> + '_, FileError> {
        let deserializer = Deserializer::from_reader(&mut self.file).into_iter::<Value>();
        let iter = deserializer
            .filter_map(Result::ok)
            .flat_map(|value| match value {
                Value::Array(arr) => arr.into_iter().map(flatten_json_record),
                _ => panic!("Expected JSON array"),
            });
        Ok(iter)
    }

    fn read_parquet_records(
        &mut self,
    ) -> Result<impl Iterator<Item = Vec<String>> + '_, FileError> {
        self.file.seek(SeekFrom::Start(0))?;
        let file_reader = Arc::new(SerializedFileReader::new(self.file.get_ref().try_clone()?)?);
        let row_group_reader = file_reader.get_row_group(0)?;
        let row_iter = RowIter::from_row_group(None, row_group_reader.as_ref())?;

        // Convert rows to Vec<String>
        let records: Vec<Vec<String>> = row_iter
            .map(|row_result| match row_result {
                Ok(row) => {
                    let record: Vec<String> = (0..row.len())
                        .filter_map(|i| row.get_column_iter().nth(i))
                        .map(|(_name, value)| match value {
                            parquet::record::Field::Str(s) => s.clone(),
                            parquet::record::Field::Bytes(b) => {
                                String::from_utf8_lossy(b.as_bytes()).to_string()
                            }
                            other => other.to_string(),
                        })
                        .collect();
                    record
                }
                Err(_) => Vec::new(),
            })
            .filter(|record| !record.is_empty())
            .collect();

        Ok(records.into_iter())
    }
}

pub enum FlexRecordIter<'a> {
    Csv(Box<dyn Iterator<Item = Vec<String>> + 'a>),
    Json(Box<dyn Iterator<Item = Vec<String>> + 'a>),
    Parquet(Box<dyn Iterator<Item = Vec<String>> + 'a>),
}

impl Iterator for FlexRecordIter<'_> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            FlexRecordIter::Csv(iter) => iter.next(),
            FlexRecordIter::Json(iter) => iter.next(),
            FlexRecordIter::Parquet(iter) => iter.next(),
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
    #[error("Missing extension for file: {0}")]
    MissingExtension(String),
    #[error("Unknown file format")]
    UnknownFileFormat,
    #[error("Invalid JSON structure")]
    InvalidJsonStructure,
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Parquet error: {0}")]
    ParquetError(#[from] ParquetError),
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
        let mut reader =
            FileReader::new("tests/test.csv", Some(',')).expect("Failed to create FileReader");
        let headers = reader.headers().expect("Failed to get headers");
        assert_eq!(headers, vec!["Name", "Age", "Country"]);
    }

    #[test]
    fn test_headers_does_not_drain_records() {
        let mut reader =
            FileReader::new("tests/test.csv", Some(',')).expect("Failed to create FileReader");
        let headers = reader.headers().expect("Failed to get headers");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();
        assert_eq!(headers, vec!["Name", "Age", "Country"]);
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_records_does_not_drain_headers() {
        let mut reader =
            FileReader::new("tests/test.csv", Some(',')).expect("Failed to create FileReader");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();
        let headers = reader.headers().expect("Failed to get headers");
        assert_eq!(headers, vec!["Name", "Age", "Country"]);
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_json_headers() {
        let mut reader =
            FileReader::new("tests/test.json", None).expect("Failed to create FileReader");
        let headers = reader.headers().expect("Failed to get headers");
        assert_eq!(headers, vec!["age", "country", "name"]);
    }

    #[test]
    fn test_nested_json_headers() {
        let mut reader = FileReader::new("tests/nested_test.json", Some(','))
            .expect("Failed to create FileReader");
        let headers = reader.headers().expect("Failed to get headers");
        assert_eq!(
            headers,
            vec!["age", "bank.account", "bank.institution", "country", "name"]
        );
    }

    #[test]
    fn test_csv_records() {
        let mut reader =
            FileReader::new("tests/test.csv", Some(',')).expect("Failed to create FileReader");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], vec!["John", "30", "USA"]);
        assert_eq!(records[1], vec!["Alice", "25", "UK"]);
        assert_eq!(records[2], vec!["Bob", "40", "Canada"]);
    }

    #[test]
    fn test_json_records() {
        let mut reader =
            FileReader::new("tests/test.json", None).expect("Failed to create FileReader");
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
        let mut reader =
            FileReader::new("tests/test.tsv", Some('\t')).expect("Failed to create FileReader");
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
        let mut reader = FileReader::new("tests/inner_array_test.json", None)
            .expect("Failed to create FileReader");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], vec!["30", "USA", "John", "[\"dog\",\"cat\"]"]);
        assert_eq!(records[1], vec!["25", "UK", "Alice", "[\"rabbit\"]"]);
        assert_eq!(records[2], vec!["40", "Canada", "Bob", "[]"]);
    }

    #[test]
    fn test_json_headers_with_inner_array() {
        let mut reader = FileReader::new("tests/inner_array_test.json", None)
            .expect("Failed to create FileReader");
        let headers = reader.headers().expect("Failed to get headers");
        assert_eq!(headers, vec!["age", "country", "name", "pets"]);
    }

    #[test]
    fn test_json_records_with_mixed_key_order() {
        let mut reader = FileReader::new("tests/mixed_key_order_test.json", None)
            .expect("Failed to create FileReader");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], vec!["30", "USA", "John"]);
        assert_eq!(records[1], vec!["25", "UK", "Alice"]);
        assert_eq!(records[2], vec!["40", "Canada", "Bob"]);
    }

    #[test]
    fn test_partial_eq_file_error() {
        assert_eq!(FileError::UnknownFileFormat, FileError::UnknownFileFormat);
        assert_eq!(
            FileError::InvalidJsonStructure,
            FileError::InvalidJsonStructure
        );
        assert_eq!(
            FileError::IoError(io::Error::from(io::ErrorKind::NotFound)),
            FileError::IoError(io::Error::from(io::ErrorKind::NotFound))
        );
        assert_ne!(
            FileError::UnknownFileFormat,
            FileError::InvalidJsonStructure
        );
    }

    #[test]
    fn test_parquet_records() {
        let mut reader =
            FileReader::new("tests/test.parquet", None).expect("Failed to create FileReader");
        let records: Vec<Vec<String>> = reader.records().unwrap().collect();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0], vec!["John", "30", "USA"]);
        assert_eq!(records[1], vec!["Alice", "25", "UK"]);
        assert_eq!(records[2], vec!["Bob", "40", "Canada"]);
    }

    #[test]
    fn test_parquet_headers() {
        let mut reader =
            FileReader::new("tests/test.parquet", None).expect("Failed to create FileReader");
        let headers = reader.headers().expect("Failed to get headers");

        assert_eq!(headers, vec!["name", "age", "country"]);
    }

    #[test]
    fn test_parquet_file_format_detection() {
        let format = FileFormat::from_file("tests/test.parquet", None)
            .expect("Failed to detect file format");

        match format {
            FileFormat::Parquet => assert!(true),
            _ => panic!("Expected Parquet format"),
        }
    }
}
