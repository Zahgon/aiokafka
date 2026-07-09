from collections.abc import Callable, Iterable

from aiokafka.util import NO_EXTENSIONS

from ._crc32c import crc as crc32c_py


def encode_varint_py(value: int, write: Callable[[int], None]) -> int:
    pass


def size_of_varint_py(value: int) -> int:
    pass


def decode_varint_py(buffer: bytearray, pos: int = 0) -> tuple[int, int]:
    pass


def calc_crc32c_py(memview: Iterable[int]) -> int:
    pass


calc_crc32c: Callable[[bytes | bytearray], int]
decode_varint: Callable[[bytearray, int], tuple[int, int]]
size_of_varint: Callable[[int], int]
encode_varint: Callable[[int, Callable[[int], None]], int]

if NO_EXTENSIONS:
    calc_crc32c = calc_crc32c_py
    decode_varint = decode_varint_py
    size_of_varint = size_of_varint_py
    encode_varint = encode_varint_py
else:
    try:
        from ._crecords import (
            crc32c_cython,
            decode_varint_cython,
            encode_varint_cython,
            size_of_varint_cython,
        )

        decode_varint = decode_varint_cython
        encode_varint = encode_varint_cython
        size_of_varint = size_of_varint_cython
        calc_crc32c = crc32c_cython
    except ImportError:  # pragma: no cover
        calc_crc32c = calc_crc32c_py
        decode_varint = decode_varint_py
        size_of_varint = size_of_varint_py
        encode_varint = encode_varint_py
