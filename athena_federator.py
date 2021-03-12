from abc import ABCMeta, abstractmethod
import base64
from uuid import uuid4

import pyarrow as pa

# https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/src/main/java/com/amazonaws/athena/connector/lambda/handlers/FederationCapabilities.java#L33
CAPABILITIES = 23


class PingResponse:
    def __init__(self, catalogName, queryId, sourceType) -> None:
        self.catalogName = catalogName
        self.queryId = queryId
        self.sourceType = sourceType

    def as_dict(self):
        return {
            "@type": "PingResponse",
            "catalogName":  self.catalogName,
            "queryId": self.queryId,
            "sourceType": self.sourceType,
            "capabilities": CAPABILITIES
        }


class ListSchemasResponse:
    requestType = 'LIST_SCHEMAS'

    def __init__(self, catalogName, schemas) -> None:
        self.catalogName = catalogName
        self.schemas = schemas

    def as_dict(self):
        return {
            "@type": 'ListSchemasResponse',
            "catalogName": self.catalogName,
            "schemas": self.schemas,
            "requestType": self.requestType
        }


class TableDefinition:
    def __init__(self, schemaName, tableName) -> None:
        self.schemaName = schemaName
        self.tableName = tableName

    def as_dict(self):
        return {"schemaName": self.schemaName, "tableName": self.tableName}


class ListTablesResponse:
    requestType = 'LIST_TABLES'

    def __init__(self, catalogName, tableDefinitions=[]) -> None:
        self.catalogName = catalogName
        self.tables = tableDefinitions

    def addTableDefinition(self, schemaName, tableName) -> None:
        self.tables.append(TableDefinition(schemaName, tableName))

    def as_dict(self):
        return {
            "@type": "ListTablesResponse",
            "catalogName": self.catalogName,
            "tables": [t.as_dict() for t in self.tables],
            "requestType": self.requestType
        }


class GetTableResponse:
    request_type = 'GET_TABLE'

    def __init__(self, catalogName, databaseName, tableName, schema) -> None:
        self.catalogName = catalogName
        self.databaseName = databaseName
        self.tableName = tableName
        self.schema = schema

    def as_dict(self):
        return {
            "@type": "GetTableResponse",
            "catalogName": self.catalogName,
            "tableName": {'schemaName': self.databaseName, 'tableName': self.tableName},
            "schema": {"schema": AthenaSDKUtils.encode_pyarrow_object(self.schema)},
            "partitionColumns": [],
            "requestType": self.request_type
        }


class GetTableLayoutResponse:
    request_type = 'GET_TABLE_LAYOUT'

    def __init__(self, catalogName, databaseName, tableName, partitions=None) -> None:
        self.catalogName = catalogName
        self.databaseName = databaseName
        self.tableName = tableName
        self.partitions = partitions

    def encoded_partition_config(self):
        """
        Encodes the schema and each record in the partition config.
        """
        partition_keys = self.partitions.keys()
        data = [pa.array(self.partitions[key]) for key in partition_keys]
        batch = pa.RecordBatch.from_arrays(data, list(partition_keys))
        return {
            "aId": str(uuid4()),
            "schema": AthenaSDKUtils.encode_pyarrow_object(batch.schema),
            "records": AthenaSDKUtils.encode_pyarrow_object(batch)
        }

    def as_dict(self):
        # If _no_ partition_config is provided, we *must* return at least 1 partition
        # otherwise Athena will not know to retrieve data.
        if self.partitions is None:
            self.partitions = {'partitionId': [1]}
            # self.partitions = {
            #     'schema': pa.schema([('partitionId', pa.int32())]),
            #     'records': {
            #         'partitionId': [1]
            #     },
            # }

        return {
            "@type": "GetTableLayoutResponse",
            "catalogName": self.catalogName,
            "tableName": {'schemaName': self.databaseName, 'tableName': self.tableName},
            "partitions": self.encoded_partition_config(),
            "requestType": self.request_type
        }


class GetSplitsResponse:
    request_type = 'GET_SPLITS'

    def __init__(self, catalogName, splits) -> None:
        self.catalogName = catalogName
        self.splits = splits

    def as_dict(self):
        return {
            "@type": "GetSplitsResponse",
            "catalogName": self.catalogName,
            "splits": self.splits,
            "continuationToken": None,
            "requestType": self.request_type
        }


class ReadRecordsResponse:
    request_type = 'READ_RECORDS'

    def __init__(self, catalogName, schema, records) -> None:
        self.catalogName = catalogName
        self.schema = schema
        self.records = records

    def as_dict(self):
        return {
            "@type": "ReadRecordsResponse",
            "catalogName": self.catalogName,
            "records": {
                "aId": str(uuid4()),
                "schema": AthenaSDKUtils.encode_pyarrow_object(self.schema),
                "records": AthenaSDKUtils.encode_pyarrow_object(self.records)
            },
            "requestType": self.request_type
        }


class AthenaSDKUtils:
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
        pa_schema = AthenaSDKUtils.parse_encoded_schema(b64_schema)
        return pa.read_record_batch(base64.b64decode(b64_records), pa_schema)


class AthenaFederator(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, event) -> None:
        self.event = event

    @abstractmethod
    def PingRequest(self) -> PingResponse:
        """Basic ping request that returns metadata about this connector"""
        raise NotImplementedError

    @abstractmethod
    def ListSchemasRequest(self) -> ListSchemasResponse:
        """List different available databases for your connector"""
        raise NotImplementedError

    @abstractmethod
    def ListTablesRequest(self) -> ListTablesResponse:
        """List available tables in the database"""
        raise NotImplementedError

    @abstractmethod
    def GetTableRequest(self) -> GetTableResponse:
        """Get Table metadata"""
        raise NotImplementedError

    @abstractmethod
    def GetTableLayoutRequest(self) -> GetTableLayoutResponse:
        """I forget the difference between TableLayout and Splits, but for now we just return a default response."""
        raise NotImplementedError

    @abstractmethod
    def GetSplitsRequest(self) -> GetSplitsResponse:
        """The splits don't matter to Athena, it's mostly hints to pass on to ReadRecordsRequest"""
        raise NotImplementedError

    @abstractmethod
    def ReadRecordsRequest(self) -> ReadRecordsResponse:
        """The actual data!"""
        raise NotImplementedError
