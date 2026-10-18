"""Accès aux tables gold (Parquet) via DuckDB. Le contrat de schéma est dans docs/architecture.md."""

import os
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

TABLES: dict[str, list[str]] = {
    "fraud_by_hour": ["hour", "country", "category", "tx_count", "fraud_count", "fraud_rate"],
    "alerts": ["transaction_id", "card_id", "ts", "amount", "score", "rule"],
    "model_metrics": ["run_ts", "dataset", "precision", "recall", "threshold"],
}


def gold_dir() -> Path:
    """FRAUD_GOLD_DIR, sinon data/gold, sinon l'échantillon figé data/sample/gold."""
    env = os.environ.get("FRAUD_GOLD_DIR")
    if env:
        return Path(env)
    live = ROOT / "data" / "gold"
    return live if live.exists() else ROOT / "data" / "sample" / "gold"


def display_path(path: Path) -> str:
    """Chemin relatif au projet quand c'est possible (n'expose pas le dossier personnel)."""
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return path.name


def load_table(name: str, base: Path | str | None = None) -> pd.DataFrame:
    if name not in TABLES:
        raise ValueError(f"table inconnue : {name}")
    folder = (Path(base) if base else gold_dir()) / name
    if not list(folder.glob("*.parquet")):
        raise FileNotFoundError(f"aucun fichier Parquet dans {folder}")
    with duckdb.connect() as con:
        df = con.execute("SELECT * FROM read_parquet(?)", [str(folder / "*.parquet")]).df()
    missing = set(TABLES[name]) - set(df.columns)
    if missing:
        raise ValueError(f"colonnes manquantes dans {name} : {sorted(missing)}")
    return df


def filter_countries(fraud_by_hour: pd.DataFrame, countries: list[str]) -> pd.DataFrame:
    return fraud_by_hour[fraud_by_hour["country"].isin(countries)]


def kpis(fraud_by_hour: pd.DataFrame) -> dict[str, float]:
    total = int(fraud_by_hour["tx_count"].sum())
    frauds = int(fraud_by_hour["fraud_count"].sum())
    return {"transactions": total, "frauds": frauds, "fraud_rate": frauds / total if total else 0.0}


def rate_by_country(fraud_by_hour: pd.DataFrame) -> pd.DataFrame:
    g = fraud_by_hour.groupby("country", as_index=False)[["tx_count", "fraud_count"]].sum()
    g["fraud_rate"] = g["fraud_count"] / g["tx_count"]
    return g.sort_values("fraud_rate", ascending=False, ignore_index=True)


def rate_by_hour_of_day(fraud_by_hour: pd.DataFrame) -> pd.DataFrame:
    g = (
        fraud_by_hour.assign(hour_of_day=pd.to_datetime(fraud_by_hour["hour"]).dt.hour)
        .groupby("hour_of_day", as_index=False)[["tx_count", "fraud_count"]]
        .sum()
    )
    g["fraud_rate"] = g["fraud_count"] / g["tx_count"]
    return g
