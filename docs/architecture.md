# Architecture

```
generator (Scala) ─┐
creditcard.csv ────┼─> bronze (Parquet, brut) ─> silver (nettoyé, dédupliqué) ─> gold (agrégats + scores) ─> Streamlit
stream (fichiers) ─┘      Spark batch                Spark batch / Structured Streaming       Spark SQL
```

## Couches
- **Bronze** : copie brute, schéma inféré puis figé, colonne `ingested_at`.
- **Silver** : types forcés, déduplication par `transaction_id`, rejets écrits dans `silver_rejects` avec la raison.
- **Gold** : détection par règles et score, agrégats par heure, pays, catégorie, taux de fraude, alertes.

## Streaming (sans Kafka)
Le générateur dépose des fichiers JSON dans `data/stream/in/`. Spark Structured Streaming lit ce dossier (source fichiers), avec checkpoint dans `data/stream/_checkpoint/`.

## Détection
1. Règles explicables en Spark SQL : montant inhabituel pour la carte, vélocité (nombre de transactions par fenêtre), changement de pays.
2. Score statistique (z-score par carte) pour comparer avec les règles.
3. Évaluation sur `creditcard.csv` : précision et rappel.

## Contrat Scala ↔ Streamlit
Streamlit lit uniquement `data/gold/**` via DuckDB. Tables gold :
- `fraud_by_hour(hour ts, country string, category string, tx_count long, fraud_count long, fraud_rate double)`
- `alerts(transaction_id string, card_id string, ts timestamp, amount double, score double, rule string)`
- `model_metrics(run_ts timestamp, dataset string, precision double, recall double, threshold double)`

## Outils et arborescence
```
src/main/scala/   générateur, bronze, silver, gold, streaming
src/test/scala/   ScalaTest + Spark local[*]
sql/              requêtes gold
app/              streamlit_app.py
tests/            pytest + streamlit AppTest
data/sample/      échantillon figé pour CI et démo
```
Prérequis à installer : `sbt` (absent de cette machine ; Java 21 présent). Compatibilité Spark/Scala à fixer à l'installation (Spark 3.5 + Scala 2.12 ou 2.13).
