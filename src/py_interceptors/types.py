from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Iterable
from typing import Any

type TypeSpec = Any
type MaybeAwaitable[T] = T | Awaitable[T]
type MaybeAsyncIterable[T] = Iterable[T] | AsyncIterable[T]
