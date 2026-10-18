# Détection de fraude bancaire — Spark/Scala, Streamlit

Pipeline de données de bout en bout : ingestion, nettoyage, détection de fraude, exposition dans un tableau de bord. Il fonctionne en **batch** (historique) et en **streaming** (micro-batchs de fichiers).

**Stack :** Scala 2.12, Apache Spark 3.5 (SQL, Structured Streaming, MLlib), Parquet, DuckDB, Streamlit, sbt, pytest, GitHub Actions.

## Ce que fait le projet

```
générateur (Scala) ─┐
creditcard.csv ─────┼─> bronze (brut) ─> silver (typé, dédupliqué, rejets) ─> gold (scores, alertes, agrégats) ─> Streamlit
dossier de fichiers ┘                     Spark batch ou Structured Streaming          Spark SQL / MLlib           via DuckDB
```

- **Bronze :** données brutes en texte, avec date d'ingestion.
- **Silver :** types forcés, déduplication par `transaction_id`, lignes invalides écartées avec leur raison (`missing_transaction_id`, `invalid_amount`, `invalid_ts`, `invalid_label`).
- **Gold :** score de risque, alertes, taux de fraude par heure, pays et catégorie, métriques de précision et de rappel.
- **Streaming :** Spark lit un dossier de fichiers JSON (sans Kafka), dédoublonne, note chaque transaction et écrit `scored/` et `alerts/` avec un checkpoint.
- **Contrat Scala ↔ Python :** le tableau de bord lit uniquement les tables Parquet gold (schémas dans [`docs/architecture.md`](docs/architecture.md)).

## Résultats mesurés

| Jeu de données | Méthode | Précision | Rappel |
|---|---|---|---|
| `creditcard.csv` réel (284 807 transactions, 492 fraudes) | RandomForest MLlib, 80/20, graine 42 | 0,892 | 0,776 |
| Synthétique (20 000 transactions, 2 % de fraude) | Règles : pays étranger, nuit, montant | 1,000 | 0,833 |

Lecture honnête de ces chiffres :
- Le résultat réel vient d'un **seul découpage** train/test, sur environ une centaine de fraudes dans le test. L'intervalle d'incertitude est large. Une validation croisée serait plus solide.
- Le résultat synthétique est **flatteur par construction** : le générateur ne produit jamais de transaction normale de nuit ni à l'étranger. Il valide la mécanique du pipeline, pas la qualité d'un détecteur.
- Les variables V1 à V28 de `creditcard.csv` sont anonymisées par PCA : le modèle n'est pas explicable en termes métier.

## Choix techniques

- **Pas de Kafka.** Le streaming lit un dossier de fichiers déposés de façon atomique (fichier caché puis renommage). Le but est de montrer Structured Streaming, le checkpoint et la déduplication sans infrastructure.
- **Silver et streaming partagent la validation** (`Silver.flag`, `Silver.project`). Seule la déduplication diffère : une fenêtre `row_number` en batch (déterministe, idempotent), `dropDuplicates` avec état en streaming.
- **Score streaming sans état.** Les fenêtres par carte ne sont pas supportées en streaming : le montant est comparé à un seuil absolu au lieu de l'historique de la carte.
- **Parquet et DuckDB** : format colonne ouvert, lecture sans cluster côté tableau de bord.
- **Tests d'abord** sur chaque couche. Une mutation a été vérifiée sur la déduplication streaming : sans `dropDuplicates`, le test échoue.

## Lancer le projet

Prérequis : Java 17 ou 21, [sbt](https://www.scala-sbt.org), [uv](https://docs.astral.sh/uv/).

```bash
# Tests
sbt test            # Scala (les tests sur creditcard.csv sont ignorés si le fichier est absent)
uv run pytest -q    # Python

# Données réelles (optionnel, 150,8 Mo, licence CC-BY 4.0)
mkdir -p data/raw
curl -L -o data/raw/creditcard.csv https://zenodo.org/api/records/7395559/files/creditcard.csv/content

# Pipeline batch : tables gold dans data/gold (n, graine, taux de fraude, fichier réel facultatif)
sbt "runMain fraud.Pipeline data/gold 20000 42 0.02 data/raw/creditcard.csv"

# Tableau de bord (lit data/gold, sinon l'échantillon figé data/sample/gold)
uv run streamlit run app/streamlit_app.py
```

Sur Java 21, `sbt` doit tourner sur ce JDK (`JAVA_HOME`). Si Homebrew installe un JDK plus récent pour `sbt`, Spark 3.5 ne démarre pas.

## Structure

```
src/main/scala/fraud/   générateur, Bronze, Silver, Gold, CreditCardModel, StreamSource, StreamingJob, Pipeline
src/test/scala/fraud/   ScalaTest + Spark local
app/                    streamlit_app.py, gold_data.py (DuckDB)
tests/                  pytest + streamlit AppTest
data/sample/gold/       échantillon figé (CI et démo sans réseau)
docs/                   spec.md (8 critères d'acceptation), architecture.md
.github/workflows/      CI : sbt test, pytest
```

## Limites connues

- La CI n'a pas encore tourné sur GitHub : elle a été rejouée localement depuis un clone propre (23 tests Scala réussis, 2 ignorés sans `creditcard.csv`, 13 tests Python réussis).
- L'état de `dropDuplicates` croît sans limite en streaming. Une production le bornerait avec un watermark.
- Aucun déploiement ni conseil de décision : le projet détecte et expose, il ne bloque rien.

## Source des données

`creditcard.csv` : jeu de données ULB / Worldline sur les fraudes par carte bancaire (deux jours de transactions européennes de septembre 2013), miroir [Zenodo 7395559](https://zenodo.org/records/7395559), licence CC-BY 4.0. Citer la source en cas de réutilisation.
