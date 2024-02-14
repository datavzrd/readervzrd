# readervzrd

Readervzrd is a Rust library that provides utilities for reading tabular data from files without worrying if they are formatted as CSV or JSON. It offers flexible functionality to extract headers and iterate over records, supporting different file formats and structures.

## Features

- Supports uniform reading of data from CSV and JSON files.
- Extracts headers from files.
- Iterate over records
- Handling of nested JSON structures

## Installation

To use readervzrd in your Rust project, add it as a dependency in your Cargo.toml file:

```toml
[dependencies]
readervzrd = "0.1.0"
```

## Usage

```rust
use readervzrd::{FileReader, FileError};

fn main() -> Result<(), FileError> {
    // Create a FileReader for a CSV file with ',' delimiter
    let mut reader = FileReader::new("data.csv", Some(','))?;

    // Get headers from the file
    let headers = reader.headers()?;
    println!("Headers: {:?}", headers);

    // Iterate over records and process them
    for record in reader.records()? {
        println!("Record: {:?}", record);
    }

    Ok(())
}
```

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, please open an issue or create a pull request on GitHub.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.



