"""版本化打分器注册表：纯本地打分，不接外部服务。

每个版本是一个确定性函数 record -> score。任务提交时锁定版本，
重放/重试都用提交时版本；版本不存在时明确报 INCOMPATIBLE_VERSION。
"""
from __future__ import annotations

from typing import Any, Callable

from app.models import RejectReason, ServiceError

ScorerFn = Callable[[Any], float]

_REGISTRY: dict[str, ScorerFn] = {}


def register(version: str, fn: ScorerFn) -> None:
    _REGISTRY[version] = fn


def available_versions() -> list[str]:
    return sorted(_REGISTRY)


def get_scorer(version: str) -> ScorerFn:
    fn = _REGISTRY.get(version)
    if fn is None:
        raise ServiceError(
            RejectReason.INCOMPATIBLE_VERSION,
            f"scorer version {version!r} is not available; "
            f"available: {available_versions()}",
        )
    return fn


def score(version: str, record: Any) -> float:
    return get_scorer(version)(record)


def _score_v1(record: Any) -> float:
    raise NotImplementedError


def _score_v2(record: Any) -> float:
    raise NotImplementedError


register("v1", _score_v1)
register("v2", _score_v2)
