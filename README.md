# 🚀 Multisource_ELT_Spark

<p align="center">

<img src="https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python"/>

<img src="https://img.shields.io/badge/Apache%20Spark-3.x-orange?style=for-the-badge&logo=apachespark"/>

<img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker"/>

<img src="https://img.shields.io/badge/PostgreSQL-16-336791?style=for-the-badge&logo=postgresql"/>

<img src="https://img.shields.io/badge/MongoDB-7-47A248?style=for-the-badge&logo=mongodb"/>

<img src="https://img.shields.io/badge/Status-Completed-success?style=for-the-badge"/>

</p>

---

# 📖 Overview

**Multisource_ELT_Spark** is a distributed Data Engineering project implementing a complete ETL pipeline with **Apache Spark**.

The pipeline extracts data from multiple heterogeneous sources, applies business rules, performs data quality validation and produces analytical datasets following the **Bronze → Silver → Gold** architecture.

This project was developed as part of a Master's degree in Data Engineering.

---

# 🎯 Objectives

The project demonstrates how to build an industrial ETL pipeline capable of:

- Extracting data from heterogeneous sources
- Cleaning and validating datasets
- Applying business rules
- Detecting invalid records
- Creating a Reject Zone
- Producing reliable analytical tables
- Implementing a modern Data Lake architecture

---

# 🏗 System Architecture

```text
          PostgreSQL          MongoDB          JSON Files
               │                 │                 │
               └──────────┬──────┴──────┬──────────┘
                          │
                 Apache Spark Cluster
              (Driver • Master • Worker)
                          │
                          ▼
                    Bronze Layer
                          │
                          ▼
                    Silver Layer
                  ┌────────┴────────┐
                  ▼                 ▼
             Gold Layer       Reject Zone
                  │
                  ▼
      Business Intelligence / Analytics
```

---

# ⚙ Technologies

| Technology | Description |
|------------|-------------|
| Apache Spark | Distributed processing |
| PySpark | ETL implementation |
| Docker | Containerization |
| Docker Compose | Service orchestration |
| PostgreSQL | Relational database |
| MongoDB | NoSQL database |
| JSON | Semi-structured files |
| Python | Main programming language |

---

# 📂 Project Structure

```text
Multisource_ELT_Spark/

├── data/
│
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

# 🔄 ETL Workflow

## 🥉 Bronze Layer

The Bronze layer stores raw data exactly as received.

Sources:

- PostgreSQL
- MongoDB
- JSON files

No transformations are applied.

---

## 🥈 Silver Layer

The Silver layer performs data cleaning and validation.

Business rules include:

### Customers

- Email validation

### Orders

- Negative total amounts corrected

### Products

- Product validation

### Order Items

- Negative unit prices corrected

- Negative discounts replaced by 0

- Discounts between 1 and 100 converted into percentages

- Invalid quantities rejected

### Reviews

Validation of customer reviews

### Delivery Events

Validation of delivery events

---

## 🥇 Gold Layer

The Gold layer produces clean analytical datasets used for reporting and business intelligence.

---

# ✅ Data Quality

The pipeline automatically validates:

- Email format
- Quantities
- Prices
- Discounts
- Missing values
- Business constraints
- Invalid records

Invalid records are redirected to a dedicated Reject Zone.

---

# 📊 Pipeline Results

## Bronze

| Dataset | Records |
|----------|---------|
| Customers | 30 |
| Orders | 40 |
| Products | 15 |
| Order Items | 80 |
| Reviews | 30 |
| Delivery Events | 60 |

---

## Reject Zone

| Reason | Count |
|----------|------|
| Invalid Email | 4 |
| Invalid Quantity | 37 |
| Invalid Discount | 11 |
| Invalid Reviews | 10 |
| Invalid Delivery Events | 18 |

---

## ✔ Quality Checks

- Email validation

- Duplicate detection

- Null value validation

- Business rules validation

- Gold generation completed successfully

---

# 🚀 Installation

Clone the repository

```bash
git clone https://github.com/bipanda93/Multisource_ELT_Spark.git

cd Multisource_ELT_Spark
```

---

Install dependencies

```bash
pip install -r requirements.txt
```

---

Start Docker services

```bash
docker compose up -d --build
```

---

Run the ETL pipeline

```bash
python src/main.py
```

---

# 📈 Future Improvements

- Apache Airflow

- Delta Lake

- Apache Kafka

- Great Expectations

- CI/CD

- Unit Tests

- Monitoring

- Cloud Deployment

- Databricks

---

# 📚 Lessons Learned

This project provided practical experience with:

- Apache Spark

- Distributed Data Processing

- Docker

- PostgreSQL

- MongoDB

- ETL Development

- Data Quality

- Bronze / Silver / Gold Architecture

- Business Rule Implementation

---

# 👨‍💻 Author

**Bipanda**

Master Data Engineering

GitHub

https://github.com/bipanda93

---

# ⭐ Support

If you like this project, don't forget to leave a ⭐ on the repository!

