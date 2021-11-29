use isar_core::collection::IsarCollection;
use isar_core::object::data_type::DataType;
use isar_core::object::isar_object::{IsarObject, Property};
use isar_core::object::object_builder::ObjectBuilder;
use isar_core::schema::collection_schema::{
    CollectionSchema, IndexPropertySchema, IndexSchema, IndexType, PropertySchema,
};
use isar_core::txn::IsarTxn;
use isar_core::utils::debug::verify_col;
use itertools::Itertools;

#[derive(PartialEq, Debug)]
pub struct TestObj {
    pub id: i64,
    pub byte: u8,
    pub int: i32,
    pub float: f32,
    pub double: f64,
    pub string: Option<String>,
}

impl TestObj {
    pub const ID_PROP: Property = Property::new(DataType::Long, 2);
    pub const BYTE_PROP: Property = Property::new(DataType::Byte, 10);
    pub const INT_PROP: Property = Property::new(DataType::Int, 11);
    pub const FLOAT_PROP: Property = Property::new(DataType::Float, 15);
    pub const DOUBLE_PROP: Property = Property::new(DataType::Double, 19);
    pub const STRING_PROP: Property = Property::new(DataType::String, 27);
    pub const PROPS: [Property; 6] = [
        TestObj::ID_PROP,
        TestObj::BYTE_PROP,
        TestObj::INT_PROP,
        TestObj::FLOAT_PROP,
        TestObj::DOUBLE_PROP,
        TestObj::STRING_PROP,
    ];

    pub fn new(id: i64, byte: u8, int: i32, float: f32, double: f64, string: Option<&str>) -> Self {
        TestObj {
            id,
            byte,
            int,
            float,
            double,
            string: string.map(|s| s.to_string()),
        }
    }

    pub fn default(id: i64) -> Self {
        Self::new(id, 0, 0, 0.0, 0.0, None)
    }

    pub fn id_index() -> IndexPropertySchema {
        IndexPropertySchema::new("id", IndexType::Value, None)
    }

    pub fn byte_index() -> IndexPropertySchema {
        IndexPropertySchema::new("byte", IndexType::Value, None)
    }

    pub fn int_index() -> IndexPropertySchema {
        IndexPropertySchema::new("int", IndexType::Value, None)
    }

    pub fn float_index() -> IndexPropertySchema {
        IndexPropertySchema::new("float", IndexType::Value, None)
    }

    pub fn double_index() -> IndexPropertySchema {
        IndexPropertySchema::new("double", IndexType::Value, None)
    }

    pub fn string_index(index_type: IndexType, case_sensitive: bool) -> IndexPropertySchema {
        IndexPropertySchema::new("string", index_type, Some(case_sensitive))
    }

    pub fn schema(name: &str, indexes: &[IndexSchema]) -> CollectionSchema {
        let properties = vec![
            PropertySchema::new("id", DataType::Long),
            PropertySchema::new("byte", DataType::Byte),
            PropertySchema::new("int", DataType::Int),
            PropertySchema::new("float", DataType::Float),
            PropertySchema::new("double", DataType::Double),
            PropertySchema::new("string", DataType::String),
        ];
        CollectionSchema::new(name, properties, indexes.to_vec(), vec![])
    }

    pub fn default_schema() -> CollectionSchema {
        let indexes = vec![
            IndexSchema::new(vec![Self::id_index()], false, false),
            IndexSchema::new(vec![Self::byte_index()], false, false),
            IndexSchema::new(vec![Self::int_index()], false, false),
            IndexSchema::new(vec![Self::float_index()], false, false),
            IndexSchema::new(vec![Self::double_index()], false, false),
            IndexSchema::new(
                vec![Self::string_index(IndexType::Value, true)],
                false,
                false,
            ),
        ];
        Self::schema("obj", &indexes)
    }

    pub fn to_isar(&self) -> ObjectBuilder {
        let mut builder = ObjectBuilder::new(&TestObj::PROPS, None);
        builder.write_long(self.id);
        builder.write_byte(self.byte);
        builder.write_int(self.int);
        builder.write_float(self.float);
        builder.write_double(self.double);
        builder.write_string(self.string.as_deref());
        builder
    }

    pub fn get(col: &IsarCollection, txn: &mut IsarTxn, id: i64) -> Option<Self> {
        let object = col.get(txn, id).unwrap();
        object.map(|o| TestObj::from(o))
    }

    pub fn save(&self, col: &IsarCollection, txn: &mut IsarTxn) {
        let ob = self.to_isar();
        col.put(txn, ob.finish()).unwrap();
    }

    pub fn verify(col: &IsarCollection, txn: &mut IsarTxn, objects: &[&TestObj]) {
        let builders = objects.iter().map(|o| o.to_isar()).collect_vec();
        let objects = builders.iter().map(|b| b.finish()).collect_vec();
        verify_col(col, txn, &objects);
    }
}

impl<'a> From<IsarObject<'a>> for TestObj {
    fn from(item: IsarObject) -> Self {
        TestObj {
            id: item.read_long(TestObj::ID_PROP),
            byte: item.read_byte(TestObj::BYTE_PROP),
            int: item.read_int(TestObj::INT_PROP),
            float: item.read_float(TestObj::FLOAT_PROP),
            double: item.read_double(TestObj::DOUBLE_PROP),
            string: item
                .read_string(TestObj::STRING_PROP)
                .map(|s| s.to_string()),
        }
    }
}
