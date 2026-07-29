# MultiSourceDataCleaning

> **Pipeline ETL distribué développé avec Apache Spark (PySpark) permettant d'extraire, nettoyer, valider et transformer des données provenant de plusieurs sources selon une architecture Bronze → Silver → Gold.**

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.x-E25A1C?logo=apachespark)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql)
![MongoDB](https://img.shields.io/badge/MongoDB-7-47A248?logo=mongodb)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker)

---

# Table des matières

- Présentation du projet
- Objectifs
- Architecture
- Technologies utilisées
- Structure du projet
- Sources de données
- Pipeline ETL
- Couche Bronze
- Couche Silver
- Couche Gold
- Contrôles qualité
- Gestion des données rejetées
- Requêtes SQL analytiques
- Résultats générés
- Installation
- Exécution du pipeline
- Déroulement de l'exécution
- Points forts
- Améliorations possibles
- Auteur

---

# Présentation du projet

Ce projet met en œuvre un pipeline **ETL distribué** capable de traiter plusieurs sources de données hétérogènes.

L'objectif est de consolider des données opérationnelles provenant de :

- PostgreSQL
- MongoDB
- Fichiers JSON

afin de produire une **couche Gold** propre, validée et prête pour l'analyse.

Le projet suit l'architecture **Medallion (Bronze → Silver → Gold)**, largement utilisée dans les plateformes modernes de traitement de données.

---

# Objectifs

Le pipeline permet automatiquement de :

- Extraire des données depuis plusieurs sources.
- Nettoyer les données.
- Standardiser les formats.
- Détecter les données invalides.
- Isoler les enregistrements rejetés.
- Réaliser des contrôles qualité.
- Construire des jeux de données métiers.
- Exécuter des requêtes SQL analytiques.
- Sauvegarder les résultats au format Parquet.

---

# Architecture

```text
                  PostgreSQL
                       │
         ┌─────────────┼─────────────┐
         │             │             │
     Clients      Commandes   Lignes de commande
         │
         ▼

MongoDB ------------ Bronze ------------ JSON
 Avis clients                Événements de livraison
                       │
                       ▼
          Nettoyage et standardisation
                       │
                       ▼
                 Couche Silver
                       │
        ┌──────────────┴──────────────┐
        │                             │
 Contrôles qualité          Gestion des rejets
        │                             │
        └──────────────┬──────────────┘
                       ▼
                  Couche Gold
                       │
       customer_order_360
       sales_by_product
       customer_orders
       product_ratings
       delivery_status
       order_items_summary
       order_products_summary
       order_reviews_summary
       delivery_summary
                       │
                       ▼
           Validation des données
                       │
                       ▼
          Requêtes SQL analytiques
                       │
                       ▼
             Sauvegarde au format Parquet
```

---

# Technologies utilisées

| Technologie | Rôle |
|--------------|------|
| Python | Développement du pipeline ETL |
| Apache Spark | Traitement distribué |
| PySpark DataFrame API | Transformations de données |
| PostgreSQL | Base de données relationnelle |
| MongoDB | Base de données NoSQL |
| JSON | Source semi-structurée |
| Docker | Conteneurisation |
| Docker Compose | Orchestration des services |
| SQL | Analyses et reporting |
| Parquet | Format de stockage des résultats |

---

# Structure du projet

```text
tp_multisource/

├── config/
├── data/
│   ├── delivery_events/
│   ├── output/
│   │   ├── gold/
│   │   ├── rejects/
│   │   ├── sql_results/
│   │   └── validation_results/
│   └── reference/
│
├── sql/
│   ├── analytical_queries/
│   └── tables/
│
├── src/
│   ├── extract.py
│   ├── cleaning.py
│   ├── quality.py
│   ├── transformation.py
│   ├── load.py
│   ├── sql_runner.py
│   └── main.py
│
├── docker-compose.yml
├── requirements.txt
└── README.md

```

---

# Sources de données

## PostgreSQL

Les données relationnelles sont extraites via JDBC.

Tables utilisées :

- customers
- orders
- order_items
- products

## MongoDB

Collection utilisée :

- reviews

## JSON

Les événements de livraison sont lus depuis :

```text
data/delivery_events/
```

---

# Pipeline ETL

Le pipeline est composé de plusieurs étapes permettant de transformer des données brutes en jeux de données exploitables.

## 1. Extraction des données

Les données sont extraites depuis trois sources différentes :

- **PostgreSQL**
  - Customers
  - Orders
  - Order Items
  - Products

- **MongoDB**
  - Reviews

- **JSON**
  - Delivery Events

Toutes les données sont chargées dans des DataFrames Spark afin d'être traitées de manière distribuée.

---

# Couche Bronze

La couche **Bronze** représente les données brutes extraites depuis les différentes sources.

Aucune transformation métier n'est réalisée à cette étape.

Objectifs :

- conserver les données originales ;
- centraliser les différentes sources ;
- garantir la traçabilité des données.

---

# Couche Silver

La couche **Silver** applique toutes les opérations de nettoyage et de standardisation.

Les traitements réalisés comprennent notamment :

- suppression des doublons ;
- suppression des valeurs inutiles ;
- normalisation des identifiants ;
- nettoyage des chaînes de caractères ;
- uniformisation des statuts ;
- conversion des types de données ;
- conversion des dates ;
- standardisation des formats.

Les enregistrements invalides sont automatiquement isolés afin de ne pas impacter les traitements suivants.

---

# Contrôles qualité

Plusieurs contrôles sont exécutés automatiquement.

Les principales validations concernent :

- identifiants nuls ;
- doublons ;
- montants négatifs ;
- notes comprises entre 1 et 5 ;
- cohérence des dates ;
- cohérence des montants ;
- présence des clients ;
- présence des livraisons ;
- cohérence des statuts.

Les résultats sont sauvegardés dans :

```text
data/output/validation_results/
```

---

# Gestion des données rejetées

Les enregistrements invalides sont exportés automatiquement.

Chaque rejet contient :

- la source ;
- le motif du rejet ;
- la date de traitement ;
- les données d'origine.

Répertoire :

```text
data/output/rejects/
```

---

# Couche Gold

La couche **Gold** contient les jeux de données métiers utilisés pour l'analyse.

Les DataFrames générés sont :

| DataFrame | Description |
|-----------|-------------|
| customer_order_360 | Vue complète des commandes clients |
| sales_by_product | Chiffre d'affaires par produit |
| customer_orders | Statistiques des commandes clients |
| product_ratings | Notes moyennes des produits |
| delivery_status | Suivi des livraisons |
| order_items_summary | Synthèse des lignes de commande |
| order_products_summary | Synthèse des produits commandés |
| order_reviews_summary | Synthèse des avis |
| delivery_summary | Statistiques des livraisons |

Tous les DataFrames Gold sont enregistrés au format **Parquet**.

---

# Requêtes SQL analytiques

À chaque exécution, cinq requêtes SQL sont lancées automatiquement sur PostgreSQL.

Les rapports disponibles sont :

1. Ventes par produit
2. Commandes par client
3. Répartition des commandes par statut
4. Chiffre d'affaires mensuel
5. Meilleurs clients

Les résultats sont :

- affichés dans Spark ;
- enregistrés automatiquement.

Répertoire :

```text
data/output/sql_results/
```

---

# Résultats générés

Après chaque exécution, le projet produit automatiquement :

```text
data/output/

├── gold/
├── rejects/
├── sql_results/
└── validation_results/
```

Tous les résultats sont sauvegardés au format **Parquet**.

---

# Installation

Démarrer les services Docker :

```bash
docker compose up -d
```

---

# Exécution du pipeline

```bash
docker compose exec \
-e JAVA_TOOL_OPTIONS="-Duser.home=/tmp" \
-e PYTHONPATH=/tmp/pyhocon \
driver \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--executor-memory 1g \
--executor-cores 1 \
--conf spark.executor.instances=1 \
--conf spark.jars.ivy=/tmp/ivy-cache \
--packages org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.13:11.1.0 \
/workspace/src/main.py
```

---

# Déroulement de l'exécution

Le pipeline réalise automatiquement les opérations suivantes :

1. Initialisation de Spark.
2. Chargement des données PostgreSQL.
3. Exécution des requêtes SQL analytiques.
4. Chargement des données MongoDB.
5. Chargement des fichiers JSON.
6. Construction de la couche Bronze.
7. Nettoyage de la couche Silver.
8. Sauvegarde des données rejetées.
9. Contrôles qualité.
10. Construction des DataFrames Gold.
11. Validation finale.
12. Sauvegarde des données Gold.
13. Sauvegarde des résultats SQL.
14. Résumé d'exécution.

---

# Points forts du projet

- Architecture Bronze → Silver → Gold.
- Pipeline ETL distribué avec Apache Spark.
- Intégration de plusieurs sources de données.
- Contrôles qualité automatisés.
- Gestion des données rejetées.
- Production de DataFrames métiers.
- Requêtes SQL exécutées automatiquement.
- Export des résultats au format Parquet.
- Projet entièrement conteneurisé avec Docker.

---

# Améliorations possibles

Les évolutions envisageables sont notamment :

- intégration de Delta Lake ;
- orchestration avec Apache Airflow ;
- chargements incrémentaux ;
- mise en place de tests unitaires ;
- pipeline CI/CD ;
- tableaux de bord Power BI ou Grafana ;
- suivi de la qualité des données ;
- catalogue de données.

---

# Auteur

Projet réalisé dans le cadre d'un **TP de traitement de données multisources**.

Ce projet illustre la conception d'un pipeline ETL distribué avec **Apache Spark**, **PostgreSQL**, **MongoDB** et **Docker**, suivant une architecture **Bronze → Silver → Gold**.

