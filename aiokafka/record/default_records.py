






import struct
import time
from collections.abc import Callable, Collection, Sized
from dataclasses import dataclass
from typing import Any, final

from typing_extensions import Self

import aiokafka.codec as codecs
from aiokafka.codec import (
    gzip_decode,
    gzip_encode,
    lz4_decode,
    lz4_encode,
    snappy_decode,
    snappy_encode,
    zstd_decode,
    zstd_encode,
)
from aiokafka.errors import CorruptRecordException, UnsupportedCodecError
from aiokafka.util import NO_EXTENSIONS

from ._protocols import (
    DefaultRecordBatchBuilderProtocol,
    DefaultRecordBatchProtocol,
    DefaultRecordMetadataProtocol,
    DefaultRecordProtocol,
)
from .util import calc_crc32c, decode_varint, encode_varint, size_of_varint


class DefaultRecordBase:
    __slots__ = ()

    HEADER_STRUCT = struct.Struct(
        ">q"  # BaseOffset => Int64
        "i"  # Length => Int32
        "i"  # PartitionLeaderEpoch => Int32
        "b"  # Magic => Int8
        "I"  # CRC => Uint32
        "h"  # Attributes => Int16
        "i"  # LastOffsetDelta => Int32 // also serves as LastSequenceDelta
        "q"  # FirstTimestamp => Int64
        "q"  # MaxTimestamp => Int64
        "q"  # ProducerId => Int64
        "h"  # ProducerEpoch => Int16
        "i"  # BaseSequence => Int32
        "i"  # Records count => Int32
    )
    ATTRIBUTES_OFFSET = struct.calcsize(">qiibI")
    CRC_OFFSET = struct.calcsize(">qiib")
    AFTER_LEN_OFFSET = struct.calcsize(">qi")

    CODEC_MASK = 0x07
    CODEC_NONE = 0x00
    CODEC_GZIP = 0x01
    CODEC_SNAPPY = 0x02
    CODEC_LZ4 = 0x03
    CODEC_ZSTD = 0x04
    TIMESTAMP_TYPE_MASK = 0x08
    TRANSACTIONAL_MASK = 0x10
    CONTROL_MASK = 0x20

    LOG_APPEND_TIME = 1
    CREATE_TIME = 0

    NO_PARTITION_LEADER_EPOCH = -1

    def _assert_has_codec(self, compression_type: int) -> bool:
        if compression_type == self.CODEC_GZIP:
            checker, name = codecs.has_gzip, "gzip"
        elif compression_type == self.CODEC_SNAPPY:
            checker, name = codecs.has_snappy, "snappy"
        elif compression_type == self.CODEC_LZ4:
            checker, name = codecs.has_lz4, "lz4"
        elif compression_type == self.CODEC_ZSTD:
            checker, name = codecs.has_zstd, "zstd"
        else:
            raise UnsupportedCodecError(
                f"Unknown compression codec {compression_type:#04x}"
            )
        if not checker():
            raise UnsupportedCodecError(
                f"Libraries for {name} compression codec not found"
            )
        return True


@final
class _DefaultRecordBatchPy(DefaultRecordBase, DefaultRecordBatchProtocol):
    def __init__(self, buffer: bytes | bytearray | memoryview) -> None:
        self._buffer = bytearray(buffer)
        self._header_data: tuple[
            int, int, int, int, int, int, int, int, int, int, int, int, int
        ] = self.HEADER_STRUCT.unpack_from(self._buffer)
        self._pos = self.HEADER_STRUCT.size
        self._num_records = self._header_data[12]
        self._next_record_index = 0
        self._decompressed = False


















    def __iter__(self) -> Self:
        self._maybe_uncompress()
        return self

    def __next__(self) -> "_DefaultRecordPy":
        if self._next_record_index >= self._num_records:
            if self._pos != len(self._buffer):
                raise CorruptRecordException(
                    f"{len(self._buffer) - self._pos}"
                    " unconsumed bytes after all records consumed"
                )
            raise StopIteration
        try:
            msg = self._read_msg()
        except (ValueError, IndexError) as err:
            raise CorruptRecordException(
                f"Found invalid record structure: {err!r}"
            ) from err
        else:
            self._next_record_index += 1
        return msg



@final
@dataclass(frozen=True)
class _DefaultRecordPy(DefaultRecordProtocol):
    __slots__ = ("headers", "key", "offset", "timestamp", "timestamp_type", "value")

    offset: int
    timestamp: int
    timestamp_type: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes | None]]


    def __repr__(self) -> str:
        return (
            f"DefaultRecord(offset={self.offset!r}, timestamp={self.timestamp!r},"
            f" timestamp_type={self.timestamp_type!r}, key={self.key!r},"
            f" value={self.value!r}, headers={self.headers!r})"
        )


@final
class _DefaultRecordBatchBuilderPy(
    DefaultRecordBase, DefaultRecordBatchBuilderProtocol
):
    MAX_RECORD_OVERHEAD = 21

    def __init__(
        self,
        magic: int,
        compression_type: int,
        is_transactional: int,
        producer_id: int,
        producer_epoch: int,
        base_sequence: int,
        batch_size: int,
    ):
        assert magic >= 2
        self._magic = magic
        self._compression_type = compression_type & self.CODEC_MASK
        self._batch_size = batch_size
        self._is_transactional = bool(is_transactional)
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._base_sequence = base_sequence

        self._first_timestamp: int | None = None
        self._max_timestamp: int | None = None
        self._last_offset = 0
        self._num_records = 0

        self._buffer = bytearray(self.HEADER_STRUCT.size)

    def _get_attributes(self, include_compression_type: bool = True) -> int:
        attrs = 0
        if include_compression_type:
            attrs |= self._compression_type
        if self._is_transactional:
            attrs |= self.TRANSACTIONAL_MASK
        return attrs

    def append(
        self,
        offset: int,
        timestamp: int | None,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
        encode_varint: Callable[[int, Callable[[int], None]], int] = encode_varint,
        size_of_varint: Callable[[int], int] = size_of_varint,
        get_type: Callable[[Any], type] = type,
        type_int: type[int] = int,
        time_time: Callable[[], float] = time.time,
        byte_like: Collection[type] = (bytes, bytearray, memoryview),
        bytearray_type: type[bytearray] = bytearray,
        len_func: Callable[[Sized], int] = len,
        zero_len_varint: int = 1,
    ) -> "_DefaultRecordMetadataPy | None":
        """Write message to messageset buffer with MsgVersion 2"""
        if get_type(offset) != type_int:
            raise TypeError(offset)
        if timestamp is None:
            timestamp = type_int(time_time() * 1000)
        elif get_type(timestamp) != type_int:
            raise TypeError(timestamp)
        if not (key is None or get_type(key) in byte_like):
            raise TypeError(f"Not supported type for key: {type(key)}")
        if not (value is None or get_type(value) in byte_like):
            raise TypeError(f"Not supported type for value: {type(value)}")

        if self._first_timestamp is None:
            self._first_timestamp = timestamp
            self._max_timestamp = timestamp
            timestamp_delta = 0
            first_message = 1
        else:
            timestamp_delta = timestamp - self._first_timestamp
            first_message = 0

        message_buffer = bytearray_type(b"\x00")  # Attributes
        write_byte = message_buffer.append
        write = message_buffer.extend

        encode_varint(timestamp_delta, write_byte)
        encode_varint(offset, write_byte)

        if key is not None:
            encode_varint(len_func(key), write_byte)
            write(key)
        else:
            write_byte(zero_len_varint)

        if value is not None:
            encode_varint(len_func(value), write_byte)
            write(value)
        else:
            write_byte(zero_len_varint)

        encode_varint(len_func(headers), write_byte)

        for h_key, h_value in headers:
            h_key_bytes = h_key.encode("utf-8")
            encode_varint(len_func(h_key_bytes), write_byte)
            write(h_key_bytes)
            if h_value is not None:
                encode_varint(len_func(h_value), write_byte)
                write(h_value)
            else:
                write_byte(zero_len_varint)

        message_len = len_func(message_buffer)
        main_buffer = self._buffer

        required_size = message_len + size_of_varint(message_len)
        if (
            required_size + len_func(main_buffer) > self._batch_size
            and not first_message
        ):
            return None

        assert self._max_timestamp is not None
        self._max_timestamp = max(self._max_timestamp, timestamp)
        self._num_records += 1
        self._last_offset = offset

        encode_varint(message_len, main_buffer.append)
        main_buffer.extend(message_buffer)

        return _DefaultRecordMetadataPy(offset, required_size, timestamp)

    def _write_header(self, use_compression_type: bool = True) -> None:
        batch_len = len(self._buffer)
        self.HEADER_STRUCT.pack_into(
            self._buffer,
            0,
            0,  # BaseOffset, set by broker
            batch_len - self.AFTER_LEN_OFFSET,  # Size from here to end
            self.NO_PARTITION_LEADER_EPOCH,
            self._magic,
            0,  # CRC will be set below, as we need a filled buffer for it
            self._get_attributes(use_compression_type),
            self._last_offset,
            self._first_timestamp or 0,
            self._max_timestamp or 0,
            self._producer_id,
            self._producer_epoch,
            self._base_sequence,
            self._num_records,
        )
        crc = calc_crc32c(self._buffer[self.ATTRIBUTES_OFFSET :])
        struct.pack_into(">I", self._buffer, self.CRC_OFFSET, crc)

    def _maybe_compress(self) -> bool:
        if self._compression_type != self.CODEC_NONE:
            assert self._assert_has_codec(self._compression_type)
            header_size = self.HEADER_STRUCT.size
            data = bytes(self._buffer[header_size:])
            if self._compression_type == self.CODEC_GZIP:
                compressed = gzip_encode(data)
            elif self._compression_type == self.CODEC_SNAPPY:
                compressed = snappy_encode(data)
            elif self._compression_type == self.CODEC_LZ4:
                compressed = lz4_encode(data)
            elif self._compression_type == self.CODEC_ZSTD:
                compressed = zstd_encode(data)
            else:
                raise RuntimeError(
                    f"Invalid compression codec {self._compression_type:#04x}"
                )
            compressed_size = len(compressed)
            if len(data) <= compressed_size:
                return False
            else:
                needed_size = header_size + compressed_size
                del self._buffer[needed_size:]
                self._buffer[header_size:needed_size] = compressed
                return True
        return False

    def build(self) -> bytearray:
        send_compressed = self._maybe_compress()
        self._write_header(send_compressed)
        return self._buffer

    def size(self) -> int:
        pass


    @classmethod
    def size_of(
        cls,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> int:
        size = 0
        if key is None:
            size += 1
        else:
            key_len = len(key)
            size += size_of_varint(key_len) + key_len
        if value is None:
            size += 1
        else:
            value_len = len(value)
            size += size_of_varint(value_len) + value_len
        size += size_of_varint(len(headers))
        for h_key, h_value in headers:
            h_key_len = len(h_key.encode("utf-8"))
            size += size_of_varint(h_key_len) + h_key_len

            if h_value is None:
                size += 1
            else:
                h_value_len = len(h_value)
                size += size_of_varint(h_value_len) + h_value_len
        return size

    @classmethod
    def estimate_size_in_bytes(
        cls,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> int:
        """Get the upper bound estimate on the size of record"""
        return (
            cls.HEADER_STRUCT.size
            + cls.MAX_RECORD_OVERHEAD
            + cls.size_of(key, value, headers)
        )






@final
@dataclass(frozen=True)
class _DefaultRecordMetadataPy(DefaultRecordMetadataProtocol):
    __slots__ = ("offset", "size", "timestamp")

    offset: int
    size: int
    timestamp: int


    def __repr__(self) -> str:
        return (
            f"DefaultRecordMetadata(offset={self.offset!r},"
            f" size={self.size!r}, timestamp={self.timestamp!r})"
        )


DefaultRecordBatchBuilder: type[DefaultRecordBatchBuilderProtocol]
DefaultRecordMetadata: type[DefaultRecordMetadataProtocol]
DefaultRecordBatch: type[DefaultRecordBatchProtocol]
DefaultRecord: type[DefaultRecordProtocol]

if NO_EXTENSIONS:
    DefaultRecordBatchBuilder = _DefaultRecordBatchBuilderPy
    DefaultRecordMetadata = _DefaultRecordMetadataPy
    DefaultRecordBatch = _DefaultRecordBatchPy
    DefaultRecord = _DefaultRecordPy
else:
    try:
        from ._crecords import (
            DefaultRecord as _DefaultRecordCython,
        )
        from ._crecords import (
            DefaultRecordBatch as _DefaultRecordBatchCython,
        )
        from ._crecords import (
            DefaultRecordBatchBuilder as _DefaultRecordBatchBuilderCython,
        )
        from ._crecords import (
            DefaultRecordMetadata as _DefaultRecordMetadataCython,
        )

        DefaultRecordBatchBuilder = _DefaultRecordBatchBuilderCython
        DefaultRecordMetadata = _DefaultRecordMetadataCython
        DefaultRecordBatch = _DefaultRecordBatchCython
        DefaultRecord = _DefaultRecordCython
    except ImportError:  # pragma: no cover
        DefaultRecordBatchBuilder = _DefaultRecordBatchBuilderPy
        DefaultRecordMetadata = _DefaultRecordMetadataPy
        DefaultRecordBatch = _DefaultRecordBatchPy
        DefaultRecord = _DefaultRecordPy
