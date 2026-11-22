//! Plain Parquet pages for the user table, whose columns are already built.
use std::path::Path;

use parquet::format::{
    ColumnChunk, ColumnMetaData, CompressionCodec, ConvertedType, DataPageHeader, DateType,
    Encoding, FieldRepetitionType, FileMetaData, LogicalType, PageHeader, PageType, RowGroup,
    SchemaElement, StringType, Type,
};
use parquet::thrift::{TCompactOutputProtocol, TSerializable};

use super::UserRows;

pub(super) fn write_user_table<N: Copy>(
    path: &Path,
    rows: &UserRows<N>,
    length: impl Fn(N) -> usize,
    encode: impl Fn(N, &mut Vec<u8>),
) -> bool {
    // Keep a plain page's payload size within its signed 32-bit header.
    if rows.category.iter().flatten().any(|text| text.len() > 4096) {
        return false;
    }
    let mut table = UserTable::new(rows.names.len());
    table.names(&rows.names, length, encode);
    table.day(&rows.day);
    table.integer("sessions", &rows.sessions);
    table.integer("events", &rows.events);
    table.integer("revenue_cents", &rows.revenue);
    table.categories(&rows.category);
    table.finish(path);
    true
}

const MAGIC: &[u8; 4] = b"PAR1";
const PAGE_ROWS: usize = 65_536;

struct ColumnType {
    physical: Type,
    converted: Option<ConvertedType>,
    logical: Option<LogicalType>,
    optional: bool,
}

impl ColumnType {
    fn text(optional: bool) -> Self {
        Self {
            physical: Type::BYTE_ARRAY,
            converted: Some(ConvertedType::UTF8),
            logical: Some(LogicalType::STRING(StringType::new())),
            optional,
        }
    }

    fn date() -> Self {
        Self {
            physical: Type::INT32,
            converted: Some(ConvertedType::DATE),
            logical: Some(LogicalType::DATE(DateType::new())),
            optional: false,
        }
    }

    fn integer() -> Self {
        Self {
            physical: Type::INT64,
            converted: None,
            logical: None,
            optional: false,
        }
    }
}

pub(super) struct UserTable {
    bytes: Vec<u8>,
    schema: Vec<SchemaElement>,
    columns: Vec<ColumnChunk>,
    rows: usize,
}

impl UserTable {
    pub(super) fn new(rows: usize) -> Self {
        let mut bytes = Vec::with_capacity(rows.saturating_mul(46).saturating_add(1024));
        bytes.extend_from_slice(MAGIC);
        let root = SchemaElement {
            type_: None,
            type_length: None,
            repetition_type: None,
            name: "schema".into(),
            num_children: Some(6),
            converted_type: None,
            scale: None,
            precision: None,
            field_id: None,
            logical_type: None,
        };
        Self {
            bytes,
            schema: vec![root],
            columns: Vec::with_capacity(6),
            rows,
        }
    }

    pub(super) fn names<T: Copy>(
        &mut self,
        values: &[T],
        length: impl Fn(T) -> usize,
        encode: impl Fn(T, &mut Vec<u8>),
    ) {
        self.column(
            "user_id",
            ColumnType::text(false),
            values,
            |page| page.iter().map(|&value| 4 + length(value)).sum(),
            |page, bytes| {
                for &value in page {
                    bytes.extend_from_slice(&(length(value) as u32).to_le_bytes());
                    encode(value, bytes);
                }
            },
        );
    }

    pub(super) fn day(&mut self, values: &[i32]) {
        self.column(
            "day",
            ColumnType::date(),
            values,
            std::mem::size_of_val,
            |page, bytes| {
                #[cfg(target_endian = "little")]
                append_native_ints(bytes, page);
                #[cfg(target_endian = "big")]
                for value in page {
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
            },
        );
    }

    pub(super) fn integer(&mut self, name: &'static str, values: &[i64]) {
        self.column(
            name,
            ColumnType::integer(),
            values,
            std::mem::size_of_val,
            |page, bytes| {
                #[cfg(target_endian = "little")]
                append_native_ints(bytes, page);
                #[cfg(target_endian = "big")]
                for value in page {
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
            },
        );
    }

    pub(super) fn categories(&mut self, values: &[Option<&str>]) {
        self.column(
            "top_category",
            ColumnType::text(true),
            values,
            |page| {
                let groups = page.len().div_ceil(8);
                let header = (groups << 1) | 1;
                let levels_len = groups + varint_len(header);
                4 + levels_len
                    + page
                        .iter()
                        .flatten()
                        .map(|text| 4 + text.len())
                        .sum::<usize>()
            },
            |page, bytes| {
                // Optional flat column. DATA_PAGE uses a length prefix followed
                // by one bit-packed hybrid-RLE run of definition levels.
                let groups = page.len().div_ceil(8);
                let header = (groups << 1) | 1;
                let levels_len = groups + varint_len(header);
                bytes.extend_from_slice(&(levels_len as u32).to_le_bytes());
                write_varint(bytes, header);
                let start = bytes.len();
                bytes.resize(start + groups, 0);
                for (index, value) in page.iter().enumerate() {
                    if let Some(value) = value {
                        bytes[start + index / 8] |= 1 << (index % 8);
                        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
                        bytes.extend_from_slice(value.as_bytes());
                    }
                }
            },
        );
    }

    fn column<T>(
        &mut self,
        name: &'static str,
        ty: ColumnType,
        values: &[T],
        length: impl Fn(&[T]) -> usize,
        encode: impl Fn(&[T], &mut Vec<u8>),
    ) {
        assert_eq!(values.len(), self.rows);
        self.schema.push(SchemaElement {
            type_: Some(ty.physical),
            type_length: None,
            repetition_type: Some(if ty.optional {
                FieldRepetitionType::OPTIONAL
            } else {
                FieldRepetitionType::REQUIRED
            }),
            name: name.into(),
            num_children: None,
            converted_type: ty.converted,
            scale: None,
            precision: None,
            field_id: None,
            logical_type: ty.logical,
        });

        let offset = self.bytes.len();
        for page in values.chunks(PAGE_ROWS) {
            let size = i32::try_from(length(page)).expect("Parquet page size");
            let header = PageHeader::new(
                PageType::DATA_PAGE,
                size,
                size,
                None,
                DataPageHeader::new(
                    page.len() as i32,
                    Encoding::PLAIN,
                    Encoding::RLE,
                    Encoding::RLE,
                    None,
                ),
                None,
                None,
                None,
            );
            self.serialize(&header);
            let start = self.bytes.len();
            encode(page, &mut self.bytes);
            debug_assert_eq!(self.bytes.len() - start, size as usize);
        }
        let size = (self.bytes.len() - offset) as i64;
        let meta = ColumnMetaData::new(
            ty.physical,
            vec![Encoding::PLAIN, Encoding::RLE],
            vec![name.into()],
            CompressionCodec::UNCOMPRESSED,
            self.rows as i64,
            size,
            size,
            None,
            offset as i64,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.columns.push(ColumnChunk::new(
            None, 0, meta, None, None, None, None, None, None,
        ));
    }

    fn serialize(&mut self, record: &impl TSerializable) {
        record
            .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut self.bytes))
            .expect("serialize Parquet metadata");
    }

    pub(super) fn finish(mut self, path: &Path) {
        assert_eq!(self.columns.len(), 6);
        let size = (self.bytes.len() - MAGIC.len()) as i64;
        let mut groups = Vec::new();
        if self.rows != 0 {
            groups.push(RowGroup::new(
                std::mem::take(&mut self.columns),
                size,
                self.rows as i64,
                None,
                None,
                None,
                None,
            ));
        }
        let metadata = FileMetaData::new(
            1,
            std::mem::take(&mut self.schema),
            self.rows as i64,
            groups,
            None,
            Some("shopping-sessions".to_owned()),
            None,
            None,
            None,
        );
        let start = self.bytes.len();
        self.serialize(&metadata);
        let footer_size = u32::try_from(self.bytes.len() - start).expect("Parquet footer size");
        self.bytes.extend_from_slice(&footer_size.to_le_bytes());
        self.bytes.extend_from_slice(MAGIC);
        std::fs::write(path, self.bytes).expect("write user_daily parquet file");
    }
}

fn varint_len(value: usize) -> usize {
    ((usize::BITS - value.leading_zeros()).max(1) as usize).div_ceil(7)
}

fn write_varint(bytes: &mut Vec<u8>, mut value: usize) {
    while value >= 128 {
        bytes.push(value as u8 | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
}

#[cfg(target_endian = "little")]
trait PlainInteger {}
#[cfg(target_endian = "little")]
impl PlainInteger for i32 {}
#[cfg(target_endian = "little")]
impl PlainInteger for i64 {}

#[cfg(target_endian = "little")]
fn append_native_ints<T: PlainInteger>(bytes: &mut Vec<u8>, values: &[T]) {
    // The implementations are initialized little-endian integers with no padding.
    let raw = unsafe {
        std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), std::mem::size_of_val(values))
    };
    bytes.extend_from_slice(raw);
}
