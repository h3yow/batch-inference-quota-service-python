"""版本化打分器注册表：纯本地确定性打分，不接外部服务。

任务提交时锁定 scorer_version，重放/重试都用提交时版本；
请求的版本不在注册表中时明确抛 INCOMPATIBLE_VERSION，绝不给出错的结果。

输入记录约定为 {"text": str, "fail": bool?}：
- "fail": true 用于测试单条失败路径；
- v1/v2 打分公式不同，用于验证版本可复现性。
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


def _check(record: Any) -> str:
    if isinstance(record, dict) and record.get("fail"):
        raise ValueError("record requested failure (fail=true)")
    if not isinstance(record, dict) or "text" not in record:
        raise ValueError("record must be an object with a 'text' field")
    return str(record["text"])


def _score_v1(record: Any) -> float:
    """v1：len(text) % 100 / 100。"""
    text = _check(record)
    return round(len(text) % 100 / 100, 4)


def _score_v2(record: Any) -> float:
    """v2：字符码和 % 100 / 100（与 v1 结果不同，便于区分版本）。"""
    text = _check(record)
    return round(sum(map(ord, text)) % 100 / 100, 4)


register("v1", _score_v1)
register("v2", _score_v2)
