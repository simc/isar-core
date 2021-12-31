#![allow(dead_code)]

use std::vec;

use isar_core::collection::IsarCollection;
use isar_core::object::data_type::DataType;
use isar_core::object::isar_object::{IsarObject, Property};
use isar_core::object::object_builder::ObjectBuilder;
use isar_core::schema::collection_schema::CollectionSchema;
use isar_core::schema::index_schema::{IndexPropertySchema, IndexSchema, IndexType};
use isar_core::schema::link_schema::LinkSchema;
use isar_core::schema::property_schema::PropertySchema;
use isar_core::txn::IsarTxn;
use itertools::Itertools;

#[derive(PartialEq, Debug)]
pub struct TestObj {
    pub id: i64,
    pub byte: u8,
    pub int: i32,
    pub float: f32,
    pub double: f64,
    pub string: Option<String>,
    pub byte_list: Option<Vec<u8>>,
    pub int_list: Option<Vec<i32>>,
    pub long_list: Option<Vec<i64>>,
    pub float_list: Option<Vec<f32>>,
    pub double_list: Option<Vec<f64>>,
    pub string_list: Option<Vec<Option<String>>>,
}

impl TestObj {
    pub const BYTE_PROP: Property = Property::new(DataType::Byte, 2);
    pub const INT_PROP: Property = Property::new(DataType::Int, 3);
    pub const LONG_PROP: Property = Property::new(DataType::Long, 7);
    pub const FLOAT_PROP: Property = Property::new(DataType::Float, 15);
    pub const DOUBLE_PROP: Property = Property::new(DataType::Double, 19);
    pub const STRING_PROP: Property = Property::new(DataType::String, 27);
    pub const BYTE_LIST_PROP: Property = Property::new(DataType::ByteList, 35);
    pub const INT_LIST_PROP: Property = Property::new(DataType::IntList, 43);
    pub const LONG_LIST_PROP: Property = Property::new(DataType::LongList, 51);
    pub const FLOAT_LIST_PROP: Property = Property::new(DataType::FloatList, 59);
    pub const DOUBLE_LIST_PROP: Property = Property::new(DataType::DoubleList, 67);
    pub const STRING_LIST_PROP: Property = Property::new(DataType::StringList, 75);
    pub const PROPS: [Property; 12] = [
        TestObj::BYTE_PROP,
        TestObj::INT_PROP,
        TestObj::LONG_PROP,
        TestObj::FLOAT_PROP,
        TestObj::DOUBLE_PROP,
        TestObj::STRING_PROP,
        TestObj::BYTE_LIST_PROP,
        TestObj::INT_LIST_PROP,
        TestObj::LONG_LIST_PROP,
        TestObj::FLOAT_LIST_PROP,
        TestObj::DOUBLE_LIST_PROP,
        TestObj::STRING_LIST_PROP,
    ];

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i64,
        byte: u8,
        int: i32,
        float: f32,
        double: f64,
        string: Option<&str>,
        byte_list: Option<&[u8]>,
        int_list: Option<&[i32]>,
        long_list: Option<&[i64]>,
        float_list: Option<&[f32]>,
        double_list: Option<&[f64]>,
        string_list: Option<&[Option<String>]>,
    ) -> Self {
        TestObj {
            id,
            byte,
            int,
            float,
            double,
            string: string.map(|s| s.to_string()),
            byte_list: byte_list.map(|l| l.to_vec()),
            int_list: int_list.map(|l| l.to_vec()),
            long_list: long_list.map(|l| l.to_vec()),
            float_list: float_list.map(|l| l.to_vec()),
            double_list: double_list.map(|l| l.to_vec()),
            string_list: string_list.map(|l| l.to_vec()),
        }
    }

    pub fn default(id: i64) -> Self {
        Self::new(id, 0, 0, 0.0, 0.0, None, None, None, None, None, None, None)
    }

    pub fn byte_index() -> IndexPropertySchema {
        IndexPropertySchema::new("byte", IndexType::Value, false)
    }

    pub fn int_index() -> IndexPropertySchema {
        IndexPropertySchema::new("int", IndexType::Value, false)
    }

    pub fn long_index() -> IndexPropertySchema {
        IndexPropertySchema::new("long", IndexType::Value, false)
    }

    pub fn float_index() -> IndexPropertySchema {
        IndexPropertySchema::new("float", IndexType::Value, false)
    }

    pub fn double_index() -> IndexPropertySchema {
        IndexPropertySchema::new("double", IndexType::Value, false)
    }

    pub fn string_index(hash: bool, case_sensitive: bool) -> IndexPropertySchema {
        let index_type = if hash {
            IndexType::Hash
        } else {
            IndexType::Value
        };
        IndexPropertySchema::new("string", index_type, case_sensitive)
    }

    pub fn byte_list_index(hash: bool) -> IndexPropertySchema {
        let index_type = if hash {
            IndexType::Hash
        } else {
            IndexType::Value
        };
        IndexPropertySchema::new("byteList", index_type, false)
    }

    pub fn int_list_index(hash: bool) -> IndexPropertySchema {
        let index_type = if hash {
            IndexType::Hash
        } else {
            IndexType::Value
        };
        IndexPropertySchema::new("intList", index_type, false)
    }

    pub fn long_list_index(hash: bool) -> IndexPropertySchema {
        let index_type = if hash {
            IndexType::Hash
        } else {
            IndexType::Value
        };
        IndexPropertySchema::new("longList", index_type, false)
    }

    pub fn float_list_index(hash: bool) -> IndexPropertySchema {
        let index_type = if hash {
            IndexType::Hash
        } else {
            IndexType::Value
        };
        IndexPropertySchema::new("floatList", index_type, false)
    }

    pub fn double_list_index(hash: bool) -> IndexPropertySchema {
        let index_type = if hash {
            IndexType::Hash
        } else {
            IndexType::Value
        };
        IndexPropertySchema::new("doubleList", index_type, false)
    }

    pub fn string_list_index(
        hash: bool,
        hash_elements: bool,
        case_sensitive: bool,
    ) -> IndexPropertySchema {
        let index_type = if hash {
            IndexType::Hash
        } else if hash_elements {
            IndexType::HashElements
        } else {
            IndexType::Value
        };
        IndexPropertySchema::new("stringList", index_type, case_sensitive)
    }

    pub fn schema(name: &str, indexes: &[IndexSchema], links: &[LinkSchema]) -> CollectionSchema {
        let properties = vec![
            PropertySchema::new("byte", DataType::Byte),
            PropertySchema::new("int", DataType::Int),
            PropertySchema::new("long", DataType::Long),
            PropertySchema::new("float", DataType::Float),
            PropertySchema::new("double", DataType::Double),
            PropertySchema::new("string", DataType::String),
            PropertySchema::new("byteList", DataType::ByteList),
            PropertySchema::new("intList", DataType::IntList),
            PropertySchema::new("longList", DataType::LongList),
            PropertySchema::new("floatList", DataType::FloatList),
            PropertySchema::new("doubleList", DataType::DoubleList),
            PropertySchema::new("stringList", DataType::StringList),
        ];
        CollectionSchema::new(name, properties, indexes.to_vec(), links.to_vec())
    }

    pub fn default_indexes() -> Vec<IndexSchema> {
        vec![
            IndexSchema::new("byte", vec![Self::byte_index()], false),
            IndexSchema::new("int", vec![Self::int_index()], false),
            IndexSchema::new("long", vec![Self::long_index()], false),
            IndexSchema::new("float", vec![Self::float_index()], false),
            IndexSchema::new("double", vec![Self::double_index()], false),
            IndexSchema::new("string", vec![Self::string_index(false, true)], false),
            IndexSchema::new("byteList", vec![Self::byte_list_index(false)], false),
            IndexSchema::new("intList", vec![Self::int_list_index(false)], false),
            IndexSchema::new("longList", vec![Self::long_list_index(false)], false),
            IndexSchema::new("floatList", vec![Self::float_list_index(false)], false),
            IndexSchema::new("doubleList", vec![Self::double_list_index(false)], false),
            IndexSchema::new(
                "stringList",
                vec![Self::string_list_index(false, true, true)],
                false,
            ),
        ]
    }

    pub fn default_schema() -> CollectionSchema {
        let indexes = Self::default_indexes();
        Self::schema("obj", &indexes, &[])
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut builder = ObjectBuilder::new(&TestObj::PROPS, None);
        builder.write_byte(self.byte);
        builder.write_int(self.int);
        builder.write_long(self.id);
        builder.write_float(self.float);
        builder.write_double(self.double);
        builder.write_string(self.string.as_deref());
        builder.write_byte_list(self.byte_list.as_deref());
        builder.write_int_list(self.int_list.as_deref());
        builder.write_long_list(self.long_list.as_deref());
        builder.write_float_list(self.float_list.as_deref());
        builder.write_double_list(self.double_list.as_deref());

        let string_list = self
            .string_list
            .as_deref()
            .map(|l| l.iter().map(|e| e.as_deref()).collect_vec());
        builder.write_string_list(string_list.as_deref());
        builder.finish().as_bytes().to_vec()
    }

    pub fn get(col: &IsarCollection, txn: &mut IsarTxn, id: i64) -> Option<Self> {
        let object = col.get(txn, id).unwrap();
        object.map(TestObj::from)
    }

    pub fn save(&self, txn: &mut IsarTxn, col: &IsarCollection) {
        let bytes = self.to_bytes();
        col.put(txn, self.id, IsarObject::from_bytes(&bytes), false)
            .unwrap();
    }
}

impl<'a> From<IsarObject<'a>> for TestObj {
    fn from(item: IsarObject) -> Self {
        TestObj {
            byte: item.read_byte(TestObj::BYTE_PROP),
            int: item.read_int(TestObj::INT_PROP),
            id: item.read_long(TestObj::LONG_PROP),
            float: item.read_float(TestObj::FLOAT_PROP),
            double: item.read_double(TestObj::DOUBLE_PROP),
            string: item
                .read_string(TestObj::STRING_PROP)
                .map(|s| s.to_string()),
            byte_list: item
                .read_byte_list(TestObj::BYTE_LIST_PROP)
                .map(|l| l.to_vec()),
            int_list: item.read_int_list(TestObj::INT_LIST_PROP),
            long_list: item.read_long_list(TestObj::LONG_LIST_PROP),
            float_list: item.read_float_list(TestObj::FLOAT_LIST_PROP),
            double_list: item.read_double_list(TestObj::DOUBLE_LIST_PROP),
            string_list: item
                .read_string_list(TestObj::STRING_LIST_PROP)
                .map(|l| l.iter().map(|s| s.map(|s| s.to_string())).collect_vec()),
        }
    }
}
