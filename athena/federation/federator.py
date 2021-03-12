from abc import ABCMeta, abstractmethod

from athena.federation.models import *


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
