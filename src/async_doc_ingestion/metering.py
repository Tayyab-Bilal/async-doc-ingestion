"""Credit metering around the embedder, and the type contract that makes it safe.

The real outage: a metering wrapper delegated every call to the embedder with `__getattr__` but
did not subclass the library's embeddings base class. Vector-store libraries check
`isinstance(embedding, Embeddings)`, so every document failed. A wrapper must be a subclass.
`StrictVectorStore` below is a stand-in for that library check.
"""
from __future__ import annotations

from .providers import Embedder, EmbeddingsBase


class MeteredEmbedder(EmbeddingsBase):
    """Counts billable texts. Subclassing the base class is the whole fix."""

    def __init__(self, inner: Embedder) -> None:
        self.inner, self.credits = inner, 0

    def embed(self, texts: list[str]) -> list[list[float]]:
        self.credits += len(texts)
        return self.inner.embed(texts)


class StrictVectorStore:
    """Third-party stand-in: refuses anything that is not an `EmbeddingsBase`."""

    def __init__(self, embedding: object) -> None:
        if not isinstance(embedding, EmbeddingsBase):
            raise TypeError(f"embedding must subclass EmbeddingsBase, got {type(embedding).__name__}")
        self.embedding = embedding

    def add_texts(self, texts: list[str]) -> int:
        return len(self.embedding.embed(texts))
