"""版本化打分器注册表。

每个打分器名称下可注册多个版本；同时声明 min_compatible_version。
- 请求版本有精确注册：返回该版本函数，保证老任务按提交时版本复现。
- 无精确注册但 >= min_compatible_version：回落到当前版本函数。
- 否则抛 VersionIncompatibleError，明确报错而不是给出错误结果。
版本号采用 "MAJOR.MINOR" 语义化格式。
"""
from __future__ import annotations

import logging
from typing import Any, Callable

# 打分函数签名：payload -> score
ScoreFn = Callable[[dict[str, Any]], float]

logger = logging.getLogger(__name__)


class ScorerError(Exception):
    """打分器内部错误。"""


class VersionIncompatibleError(Exception):
    """请求版本与当前打分器版本不兼容。"""


def _ver_tuple(version: str) -> tuple[int, ...]:
    try:
        return tuple(int(p) for p in str(version).split("."))
    except ValueError as exc:
        raise VersionIncompatibleError(f"非法版本号: {version!r}") from exc


class ScorerRegistry:
    """打分器版本注册表。"""

    def __init__(self) -> None:
        # name -> {"versions": {version: fn}, "current": str,
        #          "min_compatible_version": str}
        self._scorers: dict[str, dict[str, Any]] = {}

    def register(
        self,
        name: str,
        version: str,
        fn: ScoreFn,
        min_compatible_version: str | None = None,
    ) -> None:
        entry = self._scorers.setdefault(
            name,
            {"versions": {}, "current": None, "min_compatible_version": version},
        )
        entry["versions"][version] = fn
        if entry["current"] is None or _ver_tuple(version) > _ver_tuple(
            entry["current"]
        ):
            entry["current"] = version
        if min_compatible_version is not None:
            entry["min_compatible_version"] = min_compatible_version
        logger.info(
            "scorer registered",
            extra={"scorer": name, "version": version,
                   "min_compatible_version": entry["min_compatible_version"]},
        )

    def resolve(self, name: str, version: str) -> ScoreFn:
        """解析指定版本的打分函数；版本不兼容抛 VersionIncompatibleError。"""
        entry = self._scorers.get(name)
        if entry is None:
            raise VersionIncompatibleError(f"打分器不存在: {name!r}")
        if version in entry["versions"]:
            return entry["versions"][version]
        min_ok = entry["min_compatible_version"]
        if _ver_tuple(version) >= _ver_tuple(min_ok):
            logger.info(
                "scorer version fallback to current",
                extra={"scorer": name, "requested": version,
                       "current": entry["current"]},
            )
            return entry["versions"][entry["current"]]
        raise VersionIncompatibleError(
            f"打分器 {name!r} 请求版本 {version!r} 不兼容"
            f"（最低兼容 {min_ok!r}，当前 {entry['current']!r}）"
        )

    def current_version(self, name: str) -> str | None:
        entry = self._scorers.get(name)
        return entry["current"] if entry else None


def default_scorer_v1(payload: dict[str, Any]) -> float:
    """内置示例打分器 v1：对 payload 中的数值字段求均值。"""
    values = [v for v in payload.values() if isinstance(v, (int, float))]
    if not values:
        raise ScorerError("payload 中没有数值字段")
    return sum(values) / len(values)


def default_scorer_v2(payload: dict[str, Any]) -> float:
    """内置示例打分器 v2：v1 的均值再归一化到 (0,1)（sigmoid）。"""
    import math

    mean = default_scorer_v1(payload)
    return 1.0 / (1.0 + math.exp(-mean))
