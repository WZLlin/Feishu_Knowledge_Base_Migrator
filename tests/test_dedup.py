from kb_migrator.pipeline.dedup import NearDuplicateIndex, _shingles
from kb_migrator.models import DedupVerdict

_BASE = ("Remote work policy. Employees may work remotely up to two days per week. "
         "Requests must be submitted in advance for manager approval. "
         "Overtime requires prior sign off. All equipment remains company property. ")


def test_shingles_separate_near_from_unrelated():
    near = _BASE.replace("two days", "three days")
    unrel = "Quarterly financial audit procedures and tax filing timelines."
    s1 = _shingles(_BASE)
    j_near = len(s1 & _shingles(near)) / len(s1 | _shingles(near))
    j_unrel = len(s1 & _shingles(unrel)) / len(s1 | _shingles(unrel))
    assert j_near > 0.8 and j_unrel < 0.1


def test_near_duplicate_detected():
    idx = NearDuplicateIndex(threshold=0.8)
    idx.add("orig", _BASE)
    res = idx.query(_BASE.replace("two days", "three days"))
    assert res.verdict == DedupVerdict.NEAR_DUPLICATE
    assert res.matched_key == "orig"


def test_unrelated_is_unique():
    idx = NearDuplicateIndex(threshold=0.8)
    idx.add("orig", _BASE)
    res = idx.query("Quarterly financial audit procedures and tax filing.")
    assert res.verdict == DedupVerdict.UNIQUE
