# ✈️ Databricks Flights – Architecture Médaillon

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

<img width="300" height="550" alt="architecture_project" src="https://github.com/user-attachments/assets/5325cc0f-5d2d-4484-83e8-b82a6706a364" />

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

<img width="750" height="400" alt="JOB couche Bronze" src="https://github.com/user-attachments/assets/577ae49e-2d5a-4950-8447-7960bc3cc5b6" />

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

<img width="1085" height="488" alt="Pipelines Silver" src="https://github.com/user-attachments/assets/67ce5411-7238-4cd0-aaf5-8ecef30898a1" />

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

<img width="500" height="500" alt="Pipelines Silver" src="https://github.com/user-attachments/assets/dc0e9860-b6a1-4d6f-a6cf-bdc89ded206a" />

---
# 🚀 Technologies utilisées
- Databricks
- Apache Spark
- Delta Lake
- Delta Live Tables
- Python
- SQL
---
# 🎯 Objectifs pédagogiques
- Comprendre l’architecture Médaillon
- Implémenter une ingestion incrémentale
- Mettre en place un pipeline DLT
- Gérer des clés techniques en couche Gold
- Construire un modèle décisionnel performant
---
# 📌 Améliorations possibles
- Ajout de tests unitaires
- Mise en place CI/CD
- Ajout de Data Quality avancée
- Documentation automatique
- Dashboard BI connecté à la Gold
