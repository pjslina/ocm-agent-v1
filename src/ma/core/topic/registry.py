"""TopicRegistry：以 topic_id 索引已编译 Topic。"""

from __future__ import annotations

from ma.core.topic.compiler import CompiledTopic


class TopicNotFound(KeyError):
    """biz_id 没有对应的已编译 Topic（设计书 §4.5 TOPIC_NOT_FOUND）。"""


class TopicRegistry:
    def __init__(self) -> None:
        self._by_id: dict[str, CompiledTopic] = {}

    def register(self, compiled: CompiledTopic) -> None:
        if compiled.topic_id in self._by_id:
            raise ValueError(f"topic already registered: {compiled.topic_id!r}")
        self._by_id[compiled.topic_id] = compiled

    def get(self, topic_id: str) -> CompiledTopic:
        try:
            return self._by_id[topic_id]
        except KeyError:
            raise TopicNotFound(topic_id) from None

    def ids(self) -> list[str]:
        return sorted(self._by_id.keys())
