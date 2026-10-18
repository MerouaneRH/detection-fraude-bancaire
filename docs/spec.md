# Spécification — Détection de fraude bancaire

## Problème
Repérer les transactions frauduleuses sur un flux de cartes bancaires, en batch (historique) et en continu (streaming), puis exposer les résultats dans un tableau de bord.

## Données
- **Réel** : `creditcard.csv` (ULB/Worldline, Zenodo 7395559, CC-BY 4.0), 284 807 lignes, 492 fraudes, variables `V1..V28` anonymisées (PCA), `Time`, `Amount`, `Class`.
- **Synthétique** : générateur Scala, millions de transactions avec carte, marchand, pays, catégorie, heure, montant, fraude injectée.

## Critères d'acceptation (testables)
1. Le job silver est idempotent : deux exécutions sur la même entrée donnent la même sortie.
2. Le taux de lignes rejetées est inférieur à 2 % et chaque rejet est journalisé avec sa raison.
3. Le générateur est déterministe : même graine, mêmes transactions.
4. Le taux de fraude du jeu synthétique est configurable et mesuré à ±0,05 point près.
5. Le détecteur a un rappel d'au moins 0,70 sur `creditcard.csv` pour une précision d'au moins 0,80 (jeu de test séparé, seuil documenté).
6. Le job streaming traite un micro-batch sans perdre ni dupliquer de transaction (clé `transaction_id`).
7. Les tables gold respectent le schéma Parquet publié dans `docs/architecture.md`.
8. Le tableau de bord Streamlit se charge sans erreur sur l'échantillon figé `data/sample/`.

## Hors périmètre
Kafka, cloud, déploiement, conseils financiers ou décisions de blocage réelles.
