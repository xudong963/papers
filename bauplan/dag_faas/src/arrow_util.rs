use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub fn make_sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("usd", DataType::Int32, false),
        Field::new("country", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![100, 200, 150, 300])),
            Arc::new(StringArray::from(vec!["US", "IT", "IT", "FR"])),
        ],
    ).unwrap()
}

pub fn filter_country(batch: &RecordBatch, country: &str) -> RecordBatch {
    let len= batch.num_rows();
    let country_array = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
    let mask: Vec<bool> = (0..len).map(|i| country_array.value(i) == country).collect();

    let id_array = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let usd_array = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();

    let filtered_id: Vec<i32> = id_array.iter().enumerate().filter_map(|(i, v)| if mask[i] { v } else { None }).collect();
    let filtered_usd: Vec<i32> = usd_array.iter().enumerate().filter_map(|(i, v)| if mask[i] { v } else { None }).collect();
    let filtered_country: Vec<&str> = country_array.iter().enumerate().filter_map(|(i, v)| if mask[i] { Some(v.unwrap()) } else { None }).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("usd", DataType::Int32, false),
        Field::new("country", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(filtered_id)),
            Arc::new(Int32Array::from(filtered_usd)),
            Arc::new(StringArray::from(filtered_country)),
        ],
    ).unwrap()
}

pub fn groupby_sum(batch: &RecordBatch) -> RecordBatch {
    let len = batch.num_rows();
    let country_array = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
    let usd_array = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();

    let mut sum_map = std::collections::HashMap::new();
    for i in 0..len {
        let country = country_array.value(i);
        let usd = usd_array.value(i);
        *sum_map.entry(country).or_insert(0) += usd;
    }
    let countries: Vec<&str> = sum_map.keys().cloned().collect();
    let usds: Vec<i32> = countries.iter().map(|c| sum_map[*c]).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("country", DataType::Utf8, false),
        Field::new("usd_sum", DataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(countries)),
            Arc::new(Int32Array::from(usds)),
        ],
    ).unwrap()
}

// Arrow IPC serialization
pub fn batch_to_bytes(batch: &RecordBatch) -> Vec<u8> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    buf
}

// Arrow IPC deserialization
pub fn bytes_to_batch(bytes: &[u8]) -> RecordBatch {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    reader.next().unwrap().unwrap()
}
