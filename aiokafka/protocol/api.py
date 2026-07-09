from __future__ import annotations

import abc
from io import BytesIO
from types import UnionType
from typing import Any, ClassVar, Generic, TypeVar, get_args, get_origin

from aiokafka.errors import IncompatibleBrokerVersion

from .struct import Struct
from .types import Array, Int16, Int32, Schema, String


class RequestHeader_v1(Struct):
    SCHEMA = Schema(
        ("api_key", Int16),
        ("api_version", Int16),
        ("correlation_id", Int32),
        ("client_id", String("utf-8")),
    )

    def __init__(
        self,
        request: RequestStruct,
        correlation_id: int = 0,
        client_id: str = "aiokafka",
    ) -> None:
        super().__init__(
            request.API_KEY, request.API_VERSION, correlation_id, client_id
        )


class RequestHeader_v2(Struct):
    SCHEMA = Schema(
        ("api_key", Int16),
        ("api_version", Int16),
        ("correlation_id", Int32),
        ("client_id", String("utf-8")),
        tagged_fields=(),
    )

    def __init__(
        self,
        request: RequestStruct,
        correlation_id: int = 0,
        client_id: str = "aiokafka",
    ):
        super().__init__(
            request.API_KEY, request.API_VERSION, correlation_id, client_id
        )


class ResponseHeader_v0(Struct):
    SCHEMA = Schema(
        ("correlation_id", Int32),
    )


class ResponseHeader_v1(Struct):
    SCHEMA = Schema(
        ("correlation_id", Int32),
        tagged_fields=(),
    )


T = TypeVar("T", bound="RequestStruct")


class Request(abc.ABC, Generic[T]):

    API_KEY: ClassVar[int]
    ALLOW_UNKNOWN_API_VERSION: ClassVar[bool] = False

    _CLASSES: ClassVar[tuple[type[RequestStruct]]]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if not hasattr(cls, "API_KEY"):
            raise TypeError(f"{cls.__name__} must define class attributes 'API_KEY'")
        if (
            hasattr(cls, "__orig_bases__")
            and (base_classes := cls.__orig_bases__)
            and len(base_classes) == 1
        ):
            generic_type = get_args(base_classes[0])[0]
            if get_origin(generic_type) is UnionType:
                cls._CLASSES = get_args(get_args(base_classes[0])[0])
            else:
                cls._CLASSES = (generic_type,)

        else:
            raise TypeError(f"{cls.__name__} must extend Request")

    @abc.abstractmethod
    def build(self, request_struct_class: type[T]) -> T:
        pass

    def prepare(self, versions: dict[int, tuple[int, int]]) -> T:
        api_key = self.API_KEY
        if api_key not in versions:
            if self.ALLOW_UNKNOWN_API_VERSION:
                return self.build(self._CLASSES[0])  # type: ignore[arg-type]
            raise IncompatibleBrokerVersion(
                f"{self.__class__.__name__} cannot be used "
                "if the API version is unknown"
            )

        min_version, max_version = versions[api_key]
        for req_class in reversed(self._CLASSES):
            if min_version <= req_class.API_VERSION <= max_version:
                return self.build(req_class)  # type: ignore[arg-type]

        raise NotImplementedError(
            f"Support for {self.__class__.__name__} v{min_version} "
            "has not yet been added"
        )


class RequestStruct(Struct, metaclass=abc.ABCMeta):

    API_KEY: ClassVar[int]
    API_VERSION: ClassVar[int]
    RESPONSE_TYPE: ClassVar[type[Response]]
    SCHEMA: ClassVar[Schema]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if (
            not hasattr(cls, "API_KEY")
            or not hasattr(cls, "API_VERSION")
            or not hasattr(cls, "RESPONSE_TYPE")
            or not hasattr(cls, "SCHEMA")
        ):
            raise TypeError(
                f"{cls.__name__} must define class attributes "
                "'API_KEY', 'API_VERSION', 'RESPONSE_TYPE' and 'SCHEMA"
            )


    def build_request_header(
        self, correlation_id: int, client_id: str
    ) -> RequestHeader_v1 | RequestHeader_v2:
        if self.SCHEMA.has_tagged_fields:
            return RequestHeader_v2(
                self, correlation_id=correlation_id, client_id=client_id
            )
        return RequestHeader_v1(
            self, correlation_id=correlation_id, client_id=client_id
        )

    def parse_response_header(
        self, read_buffer: BytesIO | bytes
    ) -> ResponseHeader_v0 | ResponseHeader_v1:
        if self.SCHEMA.has_tagged_fields:
            return ResponseHeader_v1.decode(read_buffer)
        return ResponseHeader_v0.decode(read_buffer)


class Response(Struct, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request/response"""

    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request/response version"""



