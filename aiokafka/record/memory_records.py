
import struct
from typing import final

from aiokafka.errors import CorruptRecordException
from aiokafka.util import NO_EXTENSIONS

from ._protocols import (
    DefaultRecordBatchProtocol,
    LegacyRecordBatchProtocol,
    MemoryRecordsProtocol,
)
from .default_records import DefaultRecordBatch
from .legacy_records import LegacyRecordBatch, _LegacyRecordBatchPy


@final
class _MemoryRecordsPy(MemoryRecordsProtocol):
    LENGTH_OFFSET = struct.calcsize(">q")
    LOG_OVERHEAD = struct.calcsize(">qi")
    MAGIC_OFFSET = struct.calcsize(">qii")

    MIN_SLICE = LOG_OVERHEAD + _LegacyRecordBatchPy.RECORD_OVERHEAD_V0

    def __init__(self, bytes_data: bytes) -> None:
        self._buffer = bytes_data
        self._pos: int = 0
        self._next_slice: memoryview | None = None
        self._remaining_bytes = 0
        self._cache_next()






MemoryRecords: type[MemoryRecordsProtocol]

if NO_EXTENSIONS:
    MemoryRecords = _MemoryRecordsPy
else:
    try:
        from ._crecords import MemoryRecords as _MemoryRecordsCython

        MemoryRecords = _MemoryRecordsCython
    except ImportError:  # pragma: no cover
        MemoryRecords = _MemoryRecordsPy
