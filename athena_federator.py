from abc import ABCMeta, abstractmethod
import base64
from uuid import uuid4

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
        pya_schema = base64.b64encode(self.schema.serialize().slice(4)).decode("utf-8")

        return {
            "@type": "GetTableResponse",
            "catalogName": self.catalogName,
            "tableName": {'schemaName': self.databaseName, 'tableName': self.tableName},
            "schema": {"schema": pya_schema},
            "partitionColumns": [],
            "requestType": self.request_type
        }


class GetTableLayoutResponse:
    request_type = 'GET_TABLE_LAYOUT'

    def __init__(self, catalogName, databaseName, tableName, partition_config) -> None:
        self.catalogName = catalogName
        self.databaseName = databaseName
        self.tableName = tableName
        self.partition_config = partition_config

    def as_dict(self):
        return {
            "@type": "GetTableLayoutResponse",
            "catalogName": self.catalogName,
            "tableName": {'schemaName': self.databaseName, 'tableName': self.tableName},
            "partitions": self.partition_config,
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
        pya_schema = base64.b64encode(self.schema.serialize().slice(4)).decode("utf-8")
        pya_records = base64.b64encode(self.records.serialize().slice(4)).decode("utf-8")

        return {
            "@type": "ReadRecordsResponse",
            "catalogName": self.catalogName,
            "records": {
                "aId": str(uuid4()),
                "schema": pya_schema,
                "records": pya_records
            },
            "requestType": self.request_type
        }


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
