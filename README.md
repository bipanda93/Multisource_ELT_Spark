# 🚀 Multisource_ELT_Spark

<p align="center">

![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange?style=for-the-badge&logo=apachespark)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=for-the-badge&logo=postgresql)
![MongoDB](https://img.shields.io/badge/MongoDB-7-47A248?style=for-the-badge&logo=mongodb)
![Statut](https://img.shields.io/badge/Projet-Terminé-success?style=for-the-badge)

</p>

---

# 📖 Présentation

**Multisource_ELT_Spark** est un projet de **Data Engineering** mettant en œuvre un pipeline **ETL distribué** avec **Apache Spark**.

Le projet consiste à extraire des données provenant de plusieurs sources hétérogènes, à appliquer des règles métier, à contrôler la qualité des données puis à produire des jeux de données analytiques selon une architecture **Bronze → Silver → Gold**.

Ce projet a été réalisé dans le cadre du Mastère Data Engineering.

---

# 🎯 Objectifs

Le pipeline permet de :

- Extraire des données depuis plusieurs sources
- Nettoyer et normaliser les données
- Contrôler leur qualité
- Appliquer des règles métier
- Gérer les données rejetées
- Produire des tables analytiques fiables
- Illustrer une architecture Data Lake moderne

---

# 🏗️ Architecture

```text
        PostgreSQL        MongoDB        Fichiers JSON
             │               │                │
             └───────┬───────┴───────┬────────┘
                     │
             Cluster Apache Spark
         (Driver • Master • Worker)
                     │
                     ▼
               Couche Bronze
                     │
                     ▼
               Couche Silver
              ┌────────┴────────┐
              ▼                 ▼
        Couche Gold      Zone de Rejets
              │
              ▼
      Reporting & Analyse
```

---

# ⚙️ Technologies utilisées

| Technologie | Description |
|-------------|-------------|
| Apache Spark | Traitement distribué |
| PySpark | Développement du pipeline ETL |
| Docker | Conteneurisation |
| Docker Compose | Orchestration des services |
| PostgreSQL | Base de données relationnelle |
| MongoDB | Base de données NoSQL |
| JSON | Source de données semi-structurée |
| Python | Langage principal |

---

# 📂 Structure du projet

```text
Multisource_ELT_Spark
│
├── data/
├── src/
│   ├── main.py
│   ├── extract.py
│   ├── cleaning.py
│   ├── quality.py
│   ├── gold.py
│   └── config.py
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# 🔄 Fonctionnement du pipeline

## 🥉 Couche Bronze

La couche Bronze stocke les données brutes telles qu'elles sont extraites depuis les différentes sources :

- PostgreSQL
- MongoDB
- Fichiers JSON

Aucune transformation n'est appliquée.

---

## 🥈 Couche Silver

Cette couche réalise le nettoyage des données et applique les règles métier.

### Clients

- Validation des adresses e-mail

### Commandes

- Correction des montants négatifs

### Produits

- Validation des informations produits

### Lignes de commandes

- Correction des prix négatifs
- Remplacement des remises négatives par 0
- Conversion automatique des remises en pourcentage
- Rejet des quantités invalides

### Avis

Validation des avis clients.

### Livraisons

Validation des événements de livraison.

---

## 🥇 Couche Gold

La couche Gold regroupe les données nettoyées destinées aux analyses décisionnelles et au reporting.

---

# ✅ Contrôle qualité

Le pipeline vérifie automatiquement :

- Le format des adresses e-mail
- Les quantités
- Les prix
- Les remises
- Les valeurs nulles
- Les règles métier

Les données invalides sont envoyées dans une **Zone de Rejets**.

---

# 📊 Résultats du pipeline

## Couche Bronze

| Jeu de données | Nombre d'enregistrements |
|----------------|-------------------------:|
| Clients | 30 |
| Commandes | 40 |
| Produits | 15 |
| Lignes de commandes | 80 |
| Avis | 30 |
| Événements de livraison | 60 |

---

## Zone de Rejets

| Motif | Nombre |
|--------|--------:|
| Adresse e-mail invalide | 4 |
| Quantité invalide | 37 |
| Remise invalide | 11 |
| Avis rejetés | 10 |
| Événements de livraison rejetés | 18 |

---

# 🚀 Installation

Cloner le dépôt :

```bash
git clone https://github.com/bipanda93/Multisource_ELT_Spark.git

cd Multisource_ELT_Spark
```

Installer les dépendances :

```bash
pip install -r requirements.txt
```

Démarrer les services :

```bash
docker compose up -d --build
```

Lancer le pipeline :

```bash
python src/main.py
```

---

# 📈 Améliorations possibles

- Apache Airflow
- Delta Lake
- Apache Kafka
- Great Expectations
- Tests unitaires
- Intégration continue (CI/CD)
- Déploiement Cloud
- Supervision et monitoring

---

# 📚 Compétences développées

Ce projet m'a permis d'acquérir des compétences en :

- Apache Spark
- PySpark
- Data Engineering
- Docker
- PostgreSQL
- MongoDB
- Architecture Bronze / Silver / Gold
- Développement de pipelines ETL
- Contrôle qualité des données
- Application de règles métier

---

# 👨‍💻 Auteur

**Bipanda Franck Ulrich**

🎓 Mastère Data Engineering - Digital school fo Paris

🔗 GitHub : https://github.com/bipanda93/Multisource_ELT_Spark

---

# ⭐ Remerciements

Merci de votre visite !

Si ce projet vous intéresse, n'hésitez pas à laisser une ⭐ sur le dépôt GitHub.
