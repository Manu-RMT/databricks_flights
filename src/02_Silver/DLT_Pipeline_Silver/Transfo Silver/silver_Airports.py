import dlt
from config_pipeline import *

# --- ZONE DE PREPARATION (STAGING) ---

# On utilise une vue pour ne pas stocker physiquement les données intermédiaires
@dlt.view(
    name="silver_flights_stage_airports",
    comment="Nettoyage initial des données de vols en provenance de Bronze"
)
def silver_flights_stage_airports():

    return (
        spark.readStream.format("delta")
        .load(BRONZE_AIRPORTS)   # Nettoyage des métadonnées techniques de l'Auto-Loader
        .drop("_rescued_data")  # Ajout d'un marqueur de temps pour le traitement (voir analyse plus bas)
        .withColumn("modifiedDate", current_timestamp())
    )

# --- ZONE DE STOCKAGE (SILVER) ---

# Création de la table cible qui supportera le CDC
dlt.create_streaming_table(
    name="silver_flights_airports",
    comment="Table Silver des aerports gérée en SCD Type 1 (Ecrasement)"
)

# Application de la logique CDC (Change Data Capture)
dlt.create_auto_cdc_flow(
    target = "silver_flights_airports",
    source = "silver_flights_stage_airports",
    keys = ["airport_id"],              # Clé unique pour identifier un vol
    sequence_by = col("modifiedDate"), # Colonne pour gérer l'ordre des updates
    stored_as_scd_type = 1             # Type 1 = On écrase les anciennes valeurs
)
 