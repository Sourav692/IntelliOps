"""Knowledge retrieval — thin wrapper over Databricks Vector Search.

Expects config constants (`VS_ENDPOINT_NAME`, `VS_INDEX_NAME`) to be in scope
via `%run ../config/config` before this module is imported.
"""

from __future__ import annotations


_index = None


def _get_index():
    """Lazy singleton — initializing the VS client is non-trivial."""
    global _index
    if _index is None:
        from databricks.vector_search.client import VectorSearchClient
        vsc = VectorSearchClient(disable_notice=True)
        _index = vsc.get_index(VS_ENDPOINT_NAME, VS_INDEX_NAME)  # noqa: F821
    return _index


def search_knowledge(query: str, num_results: int = 4) -> list[dict]:
    """Semantic search over the curated docs corpus.

    Returns a list of dicts: {doc_id, title, content, source, tags, score}.
    Falls back to an empty list if the index is unreachable so the agent can
    still answer from system tables alone.
    """
    try:
        idx = _get_index()
        resp = idx.similarity_search(
            query_text=query,
            columns=["doc_id", "title", "content", "source", "tags"],
            num_results=int(num_results),
        )
    except Exception as e:
        print(f"[knowledge] search failed: {e}")
        return []

    data = resp.get("result", {}).get("data_array", [])
    cols = [c["name"] for c in resp.get("manifest", {}).get("columns", [])]
    results: list[dict] = []
    for row in data:
        rec = dict(zip(cols, row))
        # similarity_search returns `score` as the last element when supported
        results.append(rec)
    return results
