# src/Communs/utils.py

import sys
sys.path.append("/Workspace/Users/mandu543@gmail.com/databricks-flights")

# -----------------------------
# Importer la config et transformations
# -----------------------------
from conf.config import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import NumericType
from delta.tables import DeltaTable

# -----------------------------
# Fonctions utilitaires
# -----------------------------


def normalize_dataframe(df):
   """
   Normalise le DataFrame avec les fonctions importées directement.
   - Strings : Trim, minuscules, conserve alphanumérique et '&'.
   - Autres : Remplacement des NULLs par des valeurs par défaut.
   """
   exprs = []
   for field in df.schema.fields:
       name = field.name
       dtype = field.dataType
       # Utilisation de col_expr pour désigner l'expression de la colonne en cours
       col_expr = col(name)
       # --- TRAITEMENT DES CHAINES DE CARACTÈRES ---
       if isinstance(dtype, StringType):
           # coalesce -> trim -> regexp_replace (sauf alphanum et &) 
           normalized_col = \
               regexp_replace(
                   trim(coalesce(col_expr, lit(""))),
                   r"[^a-zA-Z0-9&:\-, ]", "" # Ajout de l'espace ici pour ne pas coller les mots après le trim
               )
           
           exprs.append(normalized_col.alias(name))

       # --- TRAITEMENT DES NUMÉRIQUES ---
       elif isinstance(dtype, NumericType):
           exprs.append(coalesce(col_expr, lit(0)).alias(name))

       # --- TRAITEMENT DES DATES ---
       elif isinstance(dtype, DateType):
           exprs.append(coalesce(col_expr, to_date(lit("1900-01-01"))).alias(name))

       # --- TRAITEMENT DES TIMESTAMPS ---
       elif isinstance(dtype, TimestampType):
           exprs.append(coalesce(col_expr, to_timestamp(lit("1900-01-01 00:00:00"))).alias(name))

       else:
           # Pour les types non gérés (Booleans, Arrays, etc.)
           exprs.append(col_expr)
   return df.select(*exprs)


def getTable(spark, schema:str ,table_name:str):
    """
    Récupère la table Spark à partir du nom de la table.
    :param spark: SparkSession
    :param schema: nom du schéma
    :param table_name: nom de la table
    """
    return spark.table(schema+"."+table_name)
