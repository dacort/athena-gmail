from base64 import decode
import pyarrow as pa


from athena.federation.utils import AthenaSDKUtils
from athena.federation.models import GetTableLayoutResponse, GetTableResponse, ReadRecordsResponse

CATALOG_NAME = 'sample_catalog'
DB_NAME = 'sample_db'
TABLE_NAME = 'sample_table'


def test_get_table_response():
    schema = pa.schema([('messageId', pa.string())])
    tr = GetTableResponse(CATALOG_NAME, DB_NAME, TABLE_NAME, schema)

    # Basic validation checks
    resp = tr.as_dict()
    assert resp.get('@type') == 'GetTableResponse'
    assert resp.get('catalogName') == CATALOG_NAME

    # The more important thing we want to validate is that the encoded schema is correct
    assert resp.get('schema').get(
        'schema') == 'eAAAABAAAAAAAAoADAAGAAUACAAKAAAAAAEDAAwAAAAIAAgAAAAEAAgAAAAEAAAAAQAAABQAAAAQABQACAAGAAcADAAAABAAEAAAAAAAAQUYAAAAEAAAAAQAAAAAAAAABAAEAAQAAAAJAAAAbWVzc2FnZUlkAAAAAAAAAA=='


def test_get_table_layout_response_without_partitions():
    tlr = GetTableLayoutResponse(CATALOG_NAME, DB_NAME, TABLE_NAME)

    # Basic validation checks
    resp = tlr.as_dict()
    assert resp.get('@type') == 'GetTableLayoutResponse'
    assert resp.get('catalogName') == CATALOG_NAME
    assert resp.get('tableName') == {
        'schemaName': DB_NAME, 'tableName': TABLE_NAME}

    # Ensure the encoded partition schema is correct
    partition_config = resp.get('partitions')
    assert list(partition_config.keys()) == ['aId', 'schema', 'records']

    schema = partition_config.get('schema')
    records = partition_config.get('records')
    assert schema == 'gAAAABAAAAAAAAoADAAGAAUACAAKAAAAAAEDAAwAAAAIAAgAAAAEAAgAAAAEAAAAAQAAABQAAAAQABQACAAGAAcADAAAABAAEAAAAAAAAQIkAAAAFAAAAAQAAAAAAAAACAAMAAgABwAIAAAAAAAAAUAAAAALAAAAcGFydGl0aW9uSWQA'
    assert records == 'iAAAABQAAAAAAAAADAAWAAYABQAIAAwADAAAAAADAwAYAAAACAAAAAAAAAAAAAoAGAAMAAQACAAKAAAAPAAAABAAAAABAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAA=='

    # Assert these map back properly as well
    pya_records = AthenaSDKUtils.decode_pyarrow_records(schema, records)
    expected_records = pa.RecordBatch.from_arrays(
        [pa.array([1])], ['partitionId'])

    assert pya_records.num_columns == expected_records.num_columns
    assert pya_records.num_rows == expected_records.num_rows
    assert pya_records.schema == expected_records.schema
    assert pya_records[0].equals(expected_records[0])
    assert pya_records[0].to_pylist() == [1]


def test_read_records_response():
    # Sample schema
    pya_schema = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string())
    ])

    # Add a few fake records
    records = {
        'id': [1, 2],
        'name': ['damon', 'dacort'],
    }
    pya_records = AthenaSDKUtils.encode_pyarrow_records(pya_schema, records)

    rrr = ReadRecordsResponse(CATALOG_NAME, pya_schema, pya_records)
    resp = rrr.as_dict()
    b64_schema = resp.get('records').get('schema')
    b64_records = resp.get('records').get('records')

    # Basic validation
    assert resp.get('@type') == 'ReadRecordsResponse'
    assert resp.get('catalogName') == CATALOG_NAME
    assert b64_schema == 'qAAAABAAAAAAAAoADAAGAAUACAAKAAAAAAEDAAwAAAAIAAgAAAAEAAgAAAAEAAAAAgAAAEQAAAAEAAAA1P///wAAAQUYAAAAEAAAAAQAAAAAAAAABAAEAAQAAAAEAAAAbmFtZQAAAAAQABQACAAGAAcADAAAABAAEAAAAAAAAQIkAAAAFAAAAAQAAAAAAAAACAAMAAgABwAIAAAAAAAAAUAAAAACAAAAaWQAAA=='
    assert b64_records == 'yAAAABQAAAAAAAAADAAWAAYABQAIAAwADAAAAAADAwAYAAAAMAAAAAAAAAAAAAoAGAAMAAQACAAKAAAAbAAAABAAAAACAAAAAAAAAAAAAAAFAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAACAAAAAAAAAAEAAAAAAAAAAAAAAAAgAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAACAAAAAAAAAAAAAAAFAAAACwAAAAAAAABkYW1vbmRhY29ydAAAAAAA'

    # Reverse validation
    decoded_records = AthenaSDKUtils.decode_pyarrow_records(
        b64_schema, b64_records)
    assert decoded_records.schema.names == ['id', 'name']
    assert decoded_records[0].to_pylist() == records['id']
    assert decoded_records[1].to_pylist() == records['name']
