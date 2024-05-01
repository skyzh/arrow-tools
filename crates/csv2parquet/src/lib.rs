use arrow::csv::{reader::Format, ReaderBuilder};
use arrow_tools::seekable_reader::*;
use parquet::{
    arrow::ArrowWriter,
    basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel},
    errors::ParquetError,
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::File, io::Seek};

#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
pub enum ParquetCompression {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    LZ4,
    ZSTD,
    LZ4_RAW,
}

#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
pub enum ParquetEncoding {
    PLAIN,
    PLAIN_DICTIONARY,
    RLE,
    RLE_DICTIONARY,
    DELTA_BINARY_PACKED,
    DELTA_LENGTH_BYTE_ARRAY,
    DELTA_BYTE_ARRAY,
    BYTE_STREAM_SPLIT,
}

#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
pub enum ParquetEnabledStatistics {
    None,
    Chunk,
    Page,
}

pub struct Opts {
    /// Input CSV fil, stdin if not present.
    input: PathBuf,

    /// Output file.
    output: PathBuf,

    /// File with Arrow schema in JSON format.
    schema_file: Option<PathBuf>,

    /// The number of records to infer the schema from. All rows if not present. Setting max-read-records to zero will stop schema inference and all columns will be string typed.
    max_read_records: Option<usize>,

    /// Set whether the CSV file has headers
    header: Option<bool>,

    /// Set the CSV file's column delimiter as a byte character.
    delimiter: char,

    /// Set the compression.
    compression: Option<ParquetCompression>,

    /// Sets encoding for any column.
    encoding: Option<ParquetEncoding>,

    /// Sets data page size limit.
    data_page_size_limit: Option<usize>,

    /// Sets dictionary page size limit.
    dictionary_page_size_limit: Option<usize>,

    /// Sets write batch size.
    write_batch_size: Option<usize>,

    /// Sets max size for a row group.
    max_row_group_size: Option<usize>,

    /// Sets "created by" property.
    created_by: Option<String>,

    /// Sets flag to enable/disable dictionary encoding for any column.
    dictionary: bool,

    /// Sets flag to enable/disable statistics for any column.
    statistics: Option<ParquetEnabledStatistics>,

    /// Sets max statistics size for any column. Applicable only if statistics are enabled.
    max_statistics_size: Option<usize>,

    /// Print the schema to stderr.
    print_schema: bool,

    /// Only print the schema
    dry: bool,
}

impl Opts {
    pub fn new(input: PathBuf, output: PathBuf) -> Self {
        Self {
            input,
            output,
            schema_file: None,
            max_read_records: None,
            header: None,
            delimiter: ',',
            compression: None,
            encoding: None,
            data_page_size_limit: None,
            dictionary_page_size_limit: None,
            write_batch_size: None,
            max_row_group_size: None,
            created_by: None,
            dictionary: false,
            statistics: None,
            max_statistics_size: None,
            print_schema: false,
            dry: false,
        }
    }
}

pub fn convert(opts: Opts) -> Result<(), ParquetError> {
    let mut file = File::open(&opts.input)?;

    let mut input: Box<dyn SeekRead> = if file.rewind().is_ok() {
        Box::new(file)
    } else {
        Box::new(SeekableReader::from_unbuffered_reader(
            file,
            opts.max_read_records,
        ))
    };

    let schema = match opts.schema_file {
        Some(schema_def_file_path) => {
            let schema_file = match File::open(&schema_def_file_path) {
                Ok(file) => Ok(file),
                Err(error) => Err(ParquetError::General(format!(
                    "Error opening schema file: {schema_def_file_path:?}, message: {error}"
                ))),
            }?;
            let schema: Result<arrow::datatypes::Schema, serde_json::Error> =
                serde_json::from_reader(schema_file);
            match schema {
                Ok(schema) => Ok(schema),
                Err(err) => Err(ParquetError::General(format!(
                    "Error reading schema json: {err}"
                ))),
            }
        }
        _ => {
            let format = Format::default()
                .with_delimiter(opts.delimiter as u8)
                .with_header(opts.header.unwrap_or(true));

            match format.infer_schema(&mut input, opts.max_read_records) {
                Ok((schema, _size)) => Ok(schema),
                Err(error) => Err(ParquetError::General(format!(
                    "Error inferring schema: {error}"
                ))),
            }
        }
    }?;

    if opts.print_schema || opts.dry {
        let json = serde_json::to_string_pretty(&schema).unwrap();
        eprintln!("Schema:");
        println!("{json}");
        if opts.dry {
            return Ok(());
        }
    }

    let schema_ref = Arc::new(schema);
    let builder = ReaderBuilder::new(schema_ref)
        .with_header(opts.header.unwrap_or(true))
        .with_delimiter(opts.delimiter as u8);

    let reader = builder.build(input)?;

    let output = File::create(opts.output)?;

    let mut props = WriterProperties::builder().set_dictionary_enabled(opts.dictionary);

    if let Some(statistics) = opts.statistics {
        let statistics = match statistics {
            ParquetEnabledStatistics::Chunk => EnabledStatistics::Chunk,
            ParquetEnabledStatistics::Page => EnabledStatistics::Page,
            ParquetEnabledStatistics::None => EnabledStatistics::None,
        };

        props = props.set_statistics_enabled(statistics);
    }

    if let Some(compression) = opts.compression {
        let compression = match compression {
            ParquetCompression::UNCOMPRESSED => Compression::UNCOMPRESSED,
            ParquetCompression::SNAPPY => Compression::SNAPPY,
            ParquetCompression::GZIP => Compression::GZIP(GzipLevel::default()),
            ParquetCompression::LZO => Compression::LZO,
            ParquetCompression::BROTLI => Compression::BROTLI(BrotliLevel::default()),
            ParquetCompression::LZ4 => Compression::LZ4,
            ParquetCompression::ZSTD => Compression::ZSTD(ZstdLevel::default()),
            ParquetCompression::LZ4_RAW => Compression::LZ4_RAW,
        };

        props = props.set_compression(compression);
    }

    if let Some(encoding) = opts.encoding {
        let encoding = match encoding {
            ParquetEncoding::PLAIN => Encoding::PLAIN,
            ParquetEncoding::PLAIN_DICTIONARY => Encoding::PLAIN_DICTIONARY,
            ParquetEncoding::RLE => Encoding::RLE,
            ParquetEncoding::RLE_DICTIONARY => Encoding::RLE_DICTIONARY,
            ParquetEncoding::DELTA_BINARY_PACKED => Encoding::DELTA_BINARY_PACKED,
            ParquetEncoding::DELTA_LENGTH_BYTE_ARRAY => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            ParquetEncoding::DELTA_BYTE_ARRAY => Encoding::DELTA_BYTE_ARRAY,
            ParquetEncoding::BYTE_STREAM_SPLIT => Encoding::BYTE_STREAM_SPLIT,
        };

        props = props.set_encoding(encoding);
    }

    if let Some(size) = opts.write_batch_size {
        props = props.set_write_batch_size(size);
    }

    if let Some(size) = opts.data_page_size_limit {
        props = props.set_data_page_size_limit(size);
    }

    if let Some(size) = opts.dictionary_page_size_limit {
        props = props.set_dictionary_page_size_limit(size);
    }

    if let Some(size) = opts.dictionary_page_size_limit {
        props = props.set_dictionary_page_size_limit(size);
    }

    if let Some(size) = opts.max_row_group_size {
        props = props.set_max_row_group_size(size);
    }

    if let Some(created_by) = opts.created_by {
        props = props.set_created_by(created_by);
    }

    if let Some(size) = opts.max_statistics_size {
        props = props.set_max_statistics_size(size);
    }

    let mut writer = ArrowWriter::try_new(output, reader.schema(), Some(props.build()))?;

    for batch in reader {
        match batch {
            Ok(batch) => writer.write(&batch)?,
            Err(error) => return Err(error.into()),
        }
    }

    match writer.close() {
        Ok(_) => Ok(()),
        Err(error) => Err(error),
    }
}
