from __future__ import annotations

import pytest

from pipeline.models import InputRecord


def _valid() -> dict:
    return {
        "unique_id": "filing_001__agent_001",
        "business_name": "Acme Corp",
        "agent_name": "Jane Doe",
        "state": "NC",
    }


def test_from_dict_valid_record():
    r = InputRecord.from_dict(_valid())
    assert r.unique_id == "filing_001__agent_001"
    assert r.business_name == "Acme Corp"
    assert r.agent_name == "Jane Doe"
    assert r.state == "NC"


def test_from_dict_missing_unique_id_raises():
    d = _valid()
    del d["unique_id"]
    with pytest.raises(ValueError, match="unique_id"):
        InputRecord.from_dict(d)


def test_from_dict_whitespace_unique_id_raises():
    d = _valid()
    d["unique_id"] = "   "
    with pytest.raises(ValueError, match="unique_id"):
        InputRecord.from_dict(d)


def test_from_dict_missing_business_name_raises():
    d = _valid()
    del d["business_name"]
    with pytest.raises(ValueError, match="business_name"):
        InputRecord.from_dict(d)


def test_from_dict_none_optional_fields_coerced():
    d = _valid()
    d["agent_name"] = None
    d["jurisdiction"] = None
    r = InputRecord.from_dict(d)
    assert r.agent_name == ""
    assert r.jurisdiction == ""


def test_from_dict_extra_keys_ignored():
    d = _valid()
    d["unexpected_field"] = "should be ignored"
    r = InputRecord.from_dict(d)
    assert r.unique_id == "filing_001__agent_001"


def test_from_dict_empty_dict_raises():
    with pytest.raises(ValueError):
        InputRecord.from_dict({})
