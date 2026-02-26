import dlt
from config_pipeline import *

# --- ZONE DE PREPARATION (STAGING) ---

# On utilise une vue pour ne pas stocker physiquement les données intermédiaires
@dlt.view(
    name="silver_flights_stage_customers",
    comment="Nettoyage initial des données de vols en provenance de Bronze"
)
def silver_flights_stage_customers():

    return (
        spark.readStream.format("delta")
        .load(BRONZE_CUSTOMERS)  
        .drop("_rescued_data") # Nettoyage des métadonnées techniques de l'Auto-Loader
        .withColumn("modifiedDate", current_timestamp()) # Ajout d'un marqueur de temps pour le traitement
    )

# --- ZONE DE STOCKAGE (SILVER) ---

# Création de la table cible qui supportera le CDC
dlt.create_streaming_table(
    name="silver_flights_customers",
    comment="Table Silver des passagers gérée en SCD Type 1 (Ecrasement)"
)

# Application de la logique CDC (Change Data Capture)
dlt.create_auto_cdc_flow(
    target = "silver_flights_customers",
    source = "silver_flights_stage_customers",
    keys = ["passenger_id"],              # Clé unique pour identifier un vol
    sequence_by = col("modifiedDate"), # Colonne pour gérer l'ordre des updates
    stored_as_scd_type = 1             # Type 1 = On écrase les anciennes valeurs
)
 