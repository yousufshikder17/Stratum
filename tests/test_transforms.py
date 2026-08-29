"""Standard transforms: one cross-section per call, PIT-safe (spec §5.4)."""

import pytest

from stratum.factors.transforms import get_op, registered_ops


def xs(**values):
    return dict(values)


def test_all_standard_ops_are_registered():
    assert registered_ops() == [
        "cross_sectional_rank",
        "pct_change",
        "sector_neutralize",
        "winsorize",
        "zscore",
    ]


def test_zscore_standardizes_within_cross_section():
    out = get_op("zscore")(xs(a=1.0, b=2.0, c=3.0), {})
    assert out["a"] == pytest.approx(-1.0)
    assert out["b"] == pytest.approx(0.0, abs=1e-12)
    assert out["c"] == pytest.approx(1.0)


def test_zscore_zero_variance_maps_to_zeros():
    out = get_op("zscore")(xs(a=2.0, b=2.0, c=2.0), {})
    assert set(out.values()) == {0.0}


def test_winsorize_clips_to_empirical_quantiles():
    values = {f"e{i}": float(i) for i in range(1, 11)}  # 1..10
    values["outlier"] = 1000.0
    out = get_op("winsorize")(values, {"limits": (0.1, 0.9)})
    clipped = max(out.values())
    assert clipped < 100.0
    assert all(v <= clipped for v in out.values())


def test_rank_is_fractional_with_averaged_ties():
    out = get_op("cross_sectional_rank")(xs(a=5.0, b=1.0, c=5.0, d=10.0), {})
    assert out["b"] == pytest.approx(0.0)
    assert out["a"] == pytest.approx(out["c"])
    assert out["d"] == pytest.approx(1.0)


def test_pct_change_uses_explicit_pit_base_and_drops_missing():
    op = get_op("pct_change")
    current = xs(a=110.0, b=90.0, orphan=50.0)
    base = {"a": 100.0, "b": 0.0}
    with pytest.raises(ValueError, match="zero baseline"):
        op(current, {"base": base})


def test_pct_change_happy_path():
    out = get_op("pct_change")(
        xs(a=110.0, b=90.0),
        {"base": {"a": 100.0, "b": 120.0}},
    )
    assert out["a"] == pytest.approx(0.1)
    assert out["b"] == pytest.approx(-0.25)
    assert "orphan" not in out


def test_sector_neutralize_demeans_by_group_label():
    op = get_op("sector_neutralize")
    values = xs(a=10.0, b=20.0, c=1.0, d=3.0)
    sectors = {"a": "tech", "b": "tech", "c": "banks", "d": "banks"}
    out = op(values, {"sectors": sectors})
    assert out["a"] == pytest.approx(-5.0)
    assert out["b"] == pytest.approx(5.0)
    assert out["c"] == pytest.approx(-1.0)
    assert out["d"] == pytest.approx(1.0)


def test_sector_neutralize_excludes_unlabeled_entities():
    out = get_op("sector_neutralize")(xs(a=10.0, ghost=99.0), {"sectors": {"a": "tech"}})
    assert set(out) == {"a"}
    assert out["a"] == pytest.approx(0.0)


def test_unknown_op_lookup_raises_with_registered_names():
    with pytest.raises(KeyError, match="registered"):
        get_op("full_sample_standardize")


def test_double_registration_is_rejected():
    from stratum.factors.transforms import register_op

    with pytest.raises(ValueError, match="already registered"):

        @register_op("zscore")
        def _dup(xs, params):  # pragma: no cover - registration must fail
            raise AssertionError


def test_zscore_requires_two_entities():
    with pytest.raises(ValueError, match="at least 2"):
        get_op("zscore")(xs(solo=1.0), {})
