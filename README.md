# ✈️ Databricks Flights – Architecture Médaillon (Bronze / Silver / Gold)

## 📌 Introduction

Le but de ce projet est d'apprendre et d'appliquer les bonnes pratiques d’ingestion et de transformation de données en architecture Médaillon sur Databricks.

Nous utilisons un jeu de données lié aux vols d’avion comprenant :

- **Bookings**

- **Customers**

- **Flights**

- **Airports**

L’architecture mise en place suit le modèle :
> Bronze → Silver → Gold

avec ingestion incrémentale, traçabilité et modélisation décisionnelle.

---

# 🏗️ Architecture du projet

## 📂 Structure des répertoires

IMAGE A mettre sur GIT

---

## 🔎 Détails des dossiers

### 📁 conf

Contient :

- `config.py` → Variables globales du projet (paths, catalog, schémas, paramètres techniques)

---

### 📁 communs

- `00_SETUP` → Création du volume RAW pour stocker les fichiers CSV initiaux

- `utils.py` → Fonctions réutilisables (helpers Spark, fonctions d’écriture, gestion des logs…)

---

### 📁 queries

Fichiers SQL permettant d’interroger chaque couche :

- Bronze

- Silver

- Gold

---

### 📁 src

#### 🔹 01_Bronze

Contient les notebooks utilisés pour :

- Paramétrage des sources

- Ingestion incrémentale

- Sauvegarde au format Parquet

---

#### 🔹 02_Silver

Contient le pipeline **Delta Live Tables (DLT)** pour :

- Transformation des données

- Gestion du CDC (Change Data Capture)

- Nettoyage et normalisation

---

#### 🔹 03_Gold

Contient :

- `main` → Notebook orchestrateur

- Notebook alimentation des tables de dimensions

- Notebook alimentation de la table de fait

---

# 🥉 Étape 1 : Alimentation de la couche Bronze

## 🔄 JOB utilisé

**JOB Flights BronzeLayer**

![Job Bronze](docs/images/job_bronze.png)

### ⚙️ Process

1. Lecture des fichiers CSV depuis un Volume Databricks

2. Ajout de métadonnées techniques (date ingestion, source file…)

3. Sauvegarde au format **Parquet** dans la couche Bronze

### ✅ Avantages de la couche Bronze

- Traçabilité des fichiers ingérés

- Logs détaillés d’ingestion

- Reprocessing possible en cas d’erreur

- Conservation des données brutes

- Optimisation des performances (format Parquet)

- Historisation des données sources

---

# 🥈 Étape 2 : Alimentation de la couche Silver

## 🔄 Pipeline utilisé

**DLT_Pipeline_Silver (Delta Live Tables)**

![Pipeline Silver](docs/images/pipeline_silver.png)

### ⚙️ Process

- Lecture des tables Bronze

- Nettoyage et transformation

- Gestion du CDC

- Création de tables Silver en mode Streaming

- Application de règles de qualité (expectations)

### ✅ Avantages

- Traitement incrémental

- Contrôle qualité des données

- Monitoring intégré

- Gestion automatique des mises à jour

- Optimisation Delta Lake

---

# 🥇 Étape 3 : Alimentation de la couche Gold

![Structure Gold](docs/images/project_structure.png)

## 🔄 Process

Un notebook principal orchestre :

1. Alimentation des tables de dimensions

   - Customers

   - Airports

   - Flights

2. Alimentation de la table de faits (Bookings)

### ⚙️ Fonctionnement

- Lecture des tables Silver

- Gestion incrémentale

- Génération / gestion des clés primaires

- Stockage des nouvelles clés

- Jointures optimisées

- Modélisation en schéma en étoile ⭐

---

# 🧠 Architecture Médaillon
 
 IMAGE A METTRE 
 