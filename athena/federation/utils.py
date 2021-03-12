import base64

import pyarrow as pa


def encode_pyarrow_object(pya_obj):
    """
    Encodes either a PyArrow Schema or set of Records to Base64.
    I'm not entirely sure why, but I had to cut off the first 4 characters
    of the `serialize()` output to be compatible with the Java SDK.
    """
    return base64.b64encode(
        pya_obj.serialize().slice(4)
    ).decode('utf-8')


def parse_encoded_schema(b64_schema):
    return pa.read_schema(pa.BufferReader(base64.b64decode(b64_schema)))


def encode_pyarrow_records(pya_schema, record_hash):
    return pa.RecordBatch.from_arrays(
        [pa.array(record_hash[name]) for name in pya_schema.names],
        schema=pya_schema
    )


def decode_pyarrow_records(b64_schema, b64_records):
    """
    Decodes an encoded record set provided a similarly encoded schema.
    Returns just the records as the schema will be included with that
    """
    pa_schema = parse_encoded_schema(b64_schema)
    return pa.read_record_batch(base64.b64decode(b64_records), pa_schema)
