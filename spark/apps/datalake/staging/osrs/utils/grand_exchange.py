from pyspark.sql.types import StructType, ArrayType, StructField, StringType, LongType

def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)

    return fields

ge_item_schema = StructType([
    StructField('current_price', StringType(), True), #0
    StructField('current_trend', StringType(), True), #1
    StructField('day180_change', StringType(), True), #2
    StructField('day180_trend', StringType(), True),  #3
    StructField('day30_change', StringType(), True),  #4
    StructField('day30_trend', StringType(), True),   #5
    StructField('day90_change', StringType(), True),  #6
    StructField('day90_trend', StringType(), True),   #7
    StructField('description', StringType(), True),   #8
    StructField('icon', StringType(), True),          #9
    StructField('icon_large', StringType(), True),    #10
    StructField('id', LongType(), True),              #11
    StructField('members', StringType(), True),       #12
    StructField('name', StringType(), True),          #13
    StructField('today_price', StringType(), True),   #14
    StructField('today_trend', StringType(), True),   #15
    StructField('type', StringType(), True),          #16
    StructField('typeIcon', StringType(), True)       #17
])

