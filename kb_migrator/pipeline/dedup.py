"""去重：级联三层，便宜优先。

① 精确    ：SHA256（在编排器层直接查台账 content_sha256 命中即精确重复）。
② 近似    ：文本 shingling(词 5-gram) → datasketch MinHashLSH，候选对再用
            MinHash.jaccard() 复核，超阈值判 NEAR_DUPLICATE。
③ 语义    ：sentence-transformers 嵌入 + FAISS 近邻，cos≥阈值作为
            SEMANTIC_CANDIDATE 进人工队列（不自动删）。为「按需」重依赖，
            缺失时该层自动跳过。

本模块只做「判定」，不改台账；由编排器根据 verdict 落库、决定保留哪一份。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from ..models import DedupVerdict

_TOKEN = re.compile(r"\w+", re.UNICODE)
_WORDCHAR = re.compile(r"\w", re.UNICODE)
_CJK = re.compile(r"[一-鿿㐀-䶿豈-﫿]")

# 中文用字符级 n-gram（词级 \w+ 会把整个短句当成一个 token，个别字改动即判为完全不同）
_CJK_K = 4
_CJK_RATIO = 0.30   # word 字符里 CJK 占比超此阈值 -> 走字符级


def _shingles(text: str, k: int = 3) -> set[str]:
    """生成 shingle 集。CJK 为主用字符 4-gram，其余用词级 k-gram。

    这样中文「每周最多两天」→「每周最多三天」这类轻微改动仍能被近似去重捕获。
    """
    text = text.lower()
    word_chars = _WORDCHAR.findall(text)
    if not word_chars:
        return set()
    cjk_ratio = len(_CJK.findall(text)) / len(word_chars)
    if cjk_ratio >= _CJK_RATIO:
        chars = word_chars                    # 去标点/空白后的字符序列
        if len(chars) < _CJK_K:
            return {"".join(chars)}
        return {"".join(chars[i : i + _CJK_K]) for i in range(len(chars) - _CJK_K + 1)}
    tokens = _TOKEN.findall(text)
    if len(tokens) < k:
        return set(tokens)
    return {" ".join(tokens[i : i + k]) for i in range(len(tokens) - k + 1)}


@dataclass
class NearDupResult:
    verdict: DedupVerdict
    matched_key: Optional[str] = None
    similarity: float = 0.0


class NearDuplicateIndex:
    """基于 datasketch MinHashLSH 的近似重复索引。

    用法：对每个已提取文本的条目 add(key, text)；新条目先 query 命中即近似重复。
    缺 datasketch 时降级为「不可用」，query 恒返回 UNIQUE（不误杀）。
    """

    def __init__(self, threshold: float = 0.75, num_perm: int = 128, shingle_k: int = 3):
        self.threshold = threshold
        self.num_perm = num_perm
        self.shingle_k = shingle_k
        self._minhashes: dict[str, "object"] = {}
        self._available = True
        try:
            from datasketch import MinHashLSH

            self._lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
        except ImportError:
            self._available = False
            self._lsh = None

    @property
    def available(self) -> bool:
        return self._available

    def _minhash(self, text: str):
        from datasketch import MinHash

        m = MinHash(num_perm=self.num_perm)
        for sh in _shingles(text, self.shingle_k):
            m.update(sh.encode("utf-8"))
        return m

    def query(self, text: str) -> NearDupResult:
        if not self._available or not text.strip():
            return NearDupResult(DedupVerdict.UNIQUE)
        m = self._minhash(text)
        candidates = self._lsh.query(m)
        best_key, best_sim = None, 0.0
        for key in candidates:
            sim = m.jaccard(self._minhashes[key])   # 复核真实相似度
            if sim > best_sim:
                best_key, best_sim = key, sim
        if best_key is not None and best_sim >= self.threshold:
            return NearDupResult(DedupVerdict.NEAR_DUPLICATE, best_key, best_sim)
        return NearDupResult(DedupVerdict.UNIQUE, best_key, best_sim)

    def add(self, key: str, text: str) -> None:
        if not self._available or not text.strip():
            return
        m = self._minhash(text)
        self._minhashes[key] = m
        if key not in self._lsh:
            self._lsh.insert(key, m)


class SemanticIndex:
    """语义近邻（可选重依赖）。缺 sentence-transformers/faiss 时不可用。

    定位：周期性「疑似重复/相关内容」审查，产出 SEMANTIC_CANDIDATE 进人工队列，
    绝不自动删除。
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2", cos_threshold: float = 0.90):
        self.cos_threshold = cos_threshold
        self._available = True
        self._keys: list[str] = []
        try:
            from sentence_transformers import SentenceTransformer
            import faiss  # noqa: F401

            self._model = SentenceTransformer(model_name)
            self._index = None
        except Exception:
            self._available = False

    @property
    def available(self) -> bool:
        return self._available

    def build(self, items: list[tuple[str, str]]) -> None:
        """items: [(key, text), ...] 一次性建索引。"""
        if not self._available or not items:
            return
        import faiss
        import numpy as np

        self._keys = [k for k, _ in items]
        emb = self._model.encode(
            [t for _, t in items], normalize_embeddings=True, convert_to_numpy=True
        ).astype("float32")
        self._index = faiss.IndexFlatIP(emb.shape[1])  # 归一化后内积=cos
        self._index.add(emb)
        self._emb = emb

    def candidates(self) -> list[tuple[str, str, float]]:
        """返回所有 cos≥阈值 的相似对 [(key_a, key_b, sim)]（去重、排除自身）。"""
        if not self._available or self._index is None:
            return []
        import numpy as np

        sims, idxs = self._index.search(self._emb, 5)
        out: list[tuple[str, str, float]] = []
        seen: set[frozenset] = set()
        for i, (row_sims, row_idxs) in enumerate(zip(sims, idxs)):
            for sim, j in zip(row_sims, row_idxs):
                if i == j or sim < self.cos_threshold:
                    continue
                pair = frozenset((self._keys[i], self._keys[j]))
                if pair in seen:
                    continue
                seen.add(pair)
                out.append((self._keys[i], self._keys[j], float(sim)))
        return out
