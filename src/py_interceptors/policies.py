from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ThreadPolicy:
    """
    Named single-thread execution lane with affinity/resume semantics.

    Chains using the same policy name share one runtime-owned worker lane.
    """

    name: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ThreadPolicy requires a non-empty name")


@dataclass(frozen=True, slots=True)
class ThreadPoolPolicy:
    """
    Named worker pool.

    Pools are shared per Runtime instance.
    """

    name: str
    workers: int

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ThreadPoolPolicy requires a non-empty name")
        if self.workers <= 0:
            raise ValueError("ThreadPoolPolicy.workers must be > 0")


@dataclass(frozen=True, slots=True)
class AsyncPolicy:
    """
    Default/main async runtime when name is None.

    Named isolated async portals are owned and reused by each Runtime instance.
    ``isolated=True`` requires a non-empty name.
    """

    name: str | None = None
    isolated: bool = False

    def __post_init__(self) -> None:
        if self.name is None:
            if self.isolated:
                raise ValueError("AsyncPolicy(isolated=True) requires a name")
        elif not self.name:
            raise ValueError("AsyncPolicy requires a non-empty name")


Policy = ThreadPolicy | ThreadPoolPolicy | AsyncPolicy
