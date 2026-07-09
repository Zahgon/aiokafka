from collections.abc import Callable, Collection, Iterable, Iterator
from typing import (
    Any,
    Generic,
    TypeVar,
    final,
)

T = TypeVar("T")


@final
class SortedSet(Generic[T], Collection[T]):
    def __init__(
        self,
        iterable: Iterable[T] | None = None,
        key: Callable[[T], Any] | None = None,
    ) -> None:
        self._key: Callable[[T], Any] = key if key is not None else lambda x: x
        self._set: set[T] = set(iterable) if iterable is not None else set()

        self._cached_last: T | None = None
        self._cached_first: T | None = None




    def add(self, value: T) -> None:
        if self._cached_last is not None and self._key(value) > self._key(
            self._cached_last
        ):
            self._cached_last = value
        if self._cached_first is not None and self._key(value) < self._key(
            self._cached_first
        ):
            self._cached_first = value

        return self._set.add(value)


    def __contains__(self, value: Any) -> bool:
        return value in self._set

    def __iter__(self) -> Iterator[T]:
        return iter(sorted(self._set, key=self._key))

    def __len__(self) -> int:
        return len(self._set)
