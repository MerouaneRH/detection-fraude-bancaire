import pandas as pd
import pytest

import gold_data

SAMPLE = gold_data.ROOT / "data" / "sample" / "gold"


def _fbh(rows):
    return pd.DataFrame(rows, columns=["hour", "country", "category", "tx_count", "fraud_count", "fraud_rate"])


def test_les_trois_tables_de_l_echantillon_respectent_le_contrat():
    for name, columns in gold_data.TABLES.items():
        df = gold_data.load_table(name, SAMPLE)
        assert list(df.columns) == columns
        assert len(df) > 0


def test_table_inconnue():
    with pytest.raises(ValueError):
        gold_data.load_table("nimporte_quoi", SAMPLE)


def test_dossier_vide_ou_absent(tmp_path):
    with pytest.raises(FileNotFoundError):
        gold_data.load_table("alerts", tmp_path)


def test_colonnes_manquantes(tmp_path):
    folder = tmp_path / "alerts"
    folder.mkdir()
    pd.DataFrame({"transaction_id": ["t1"]}).to_parquet(folder / "part-0.parquet")
    with pytest.raises(ValueError, match="colonnes manquantes"):
        gold_data.load_table("alerts", tmp_path)


def test_kpis():
    df = _fbh([("2025-01-01 03:00", "FR", "fuel", 90, 1, 0.01), ("2025-01-01 04:00", "US", "fuel", 10, 4, 0.4)])
    k = gold_data.kpis(df)
    assert k == {"transactions": 100, "frauds": 5, "fraud_rate": 0.05}


def test_kpis_sans_donnees():
    assert gold_data.kpis(_fbh([]))["fraud_rate"] == 0.0


def test_taux_par_pays_trie_du_plus_au_moins_risque():
    df = _fbh(
        [
            ("2025-01-01 03:00", "FR", "fuel", 90, 1, 0.0),
            ("2025-01-02 03:00", "FR", "fuel", 10, 1, 0.0),
            ("2025-01-01 04:00", "US", "fuel", 10, 4, 0.0),
        ]
    )
    r = gold_data.rate_by_country(df)
    assert list(r["country"]) == ["US", "FR"]
    assert r.loc[r["country"] == "FR", "fraud_rate"].iloc[0] == pytest.approx(0.02)


def test_taux_par_heure_de_la_journee():
    df = _fbh(
        [
            ("2025-01-01 03:00", "FR", "fuel", 10, 5, 0.0),
            ("2025-01-02 03:00", "FR", "fuel", 10, 1, 0.0),
            ("2025-01-01 12:00", "FR", "fuel", 20, 0, 0.0),
        ]
    )
    r = gold_data.rate_by_hour_of_day(df).set_index("hour_of_day")
    assert r.loc[3, "fraud_rate"] == pytest.approx(0.3)
    assert r.loc[12, "fraud_rate"] == 0.0


def test_filtre_par_pays():
    df = _fbh([("2025-01-01 03:00", "FR", "fuel", 1, 0, 0.0), ("2025-01-01 03:00", "US", "fuel", 1, 0, 0.0)])
    assert list(gold_data.filter_countries(df, ["US"])["country"]) == ["US"]
