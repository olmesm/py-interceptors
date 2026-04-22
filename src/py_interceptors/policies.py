from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Portal:
    """
    Named execution identity shared by policies.
    """

    name: str

    def __init__(self, name: str) -> None:
        if not name:
            raise ValueError("Portal requires a non-empty name")
        object.__setattr__(self, "name", name)


def _portal_name(value: str | Portal, policy_name: str) -> str:
    if isinstance(value, Portal):
        return value.name
    if not value:
        raise ValueError(f"{policy_name} requires a non-empty name")
    return value


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    name: str


@dataclass(frozen=True, slots=True)
class ThreadPolicy(ExecutionPolicy):
    """
    Named single-thread execution lane with affinity/resume semantics.
    """

    def __init__(self, name: str | Portal) -> None:
        object.__setattr__(self, "name", _portal_name(name, "ThreadPolicy"))


@dataclass(frozen=True, slots=True)
class ThreadPoolPolicy(ExecutionPolicy):
    """
    Named worker pool.

    Pools are shared per Runtime instance.
    """

    workers: int

    def __init__(self, name: str | Portal, workers: int) -> None:
        if workers <= 0:
            raise ValueError("ThreadPoolPolicy.workers must be > 0")
        object.__setattr__(self, "name", _portal_name(name, "ThreadPoolPolicy"))
        object.__setattr__(self, "workers", workers)


@dataclass(frozen=True, slots=True)
class AsyncPolicy:
    """
    Default/main async runtime when name is None.

    Named isolated async portals are owned and reused by each Runtime instance.
    """

    name: str | None = None
    isolated: bool = False

    def __init__(
        self,
        name: str | Portal | None = None,
        isolated: bool = False,
    ) -> None:
        if name is None:
            if isolated:
                raise ValueError("AsyncPolicy(isolated=True) requires a name")
            resolved_name = None
        else:
            resolved_name = _portal_name(name, "AsyncPolicy")

        object.__setattr__(self, "name", resolved_name)
        object.__setattr__(self, "isolated", isolated)


Policy = ThreadPolicy | ThreadPoolPolicy | AsyncPolicy
