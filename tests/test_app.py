from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

import gold_data

APP = str(Path(__file__).resolve().parent.parent / "app" / "streamlit_app.py")
SAMPLE = gold_data.ROOT / "data" / "sample" / "gold"


@pytest.fixture(autouse=True)
def sample_gold(monkeypatch):
    monkeypatch.setenv("FRAUD_GOLD_DIR", str(SAMPLE))


def _int(metric) -> int:
    return int(metric.value.replace(",", ""))


def test_l_application_se_charge_sur_l_echantillon_fige():  # critère 8
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert not at.exception
    assert [m.label for m in at.metric] == ["Transactions", "Fraudes", "Taux de fraude"]
    assert _int(at.metric[0]) == 20000
    assert len(at.dataframe) == 2


def test_le_filtre_pays_reduit_les_transactions():
    at = AppTest.from_file(APP, default_timeout=30).run()
    total = _int(at.metric[0])
    at.multiselect(key="countries").set_value(["FR"]).run()
    assert not at.exception
    assert 0 < _int(at.metric[0]) < total


def test_aucun_pays_affiche_un_avertissement():
    at = AppTest.from_file(APP, default_timeout=30).run()
    at.multiselect(key="countries").set_value([]).run()
    assert not at.exception
    assert at.warning
    assert len(at.metric) == 0


def test_tables_absentes_affichent_une_erreur(monkeypatch, tmp_path):
    monkeypatch.setenv("FRAUD_GOLD_DIR", str(tmp_path))
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert at.error
    assert len(at.metric) == 0
