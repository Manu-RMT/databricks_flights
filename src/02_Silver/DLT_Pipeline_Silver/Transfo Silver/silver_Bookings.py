import dlt
from config_pipeline import *

# --- 1. VUE DE STAGING ---

# On crée une vue (non stockée physiquement) pour lire la source Bronze.
# dlt.view permet d'économiser du stockage tout en préparant la donnée.

@dlt.view(name="silver_flights_stage_bookings")

def silver_flights_stage_bookings():

    # Lecture en mode streaming depuis le dossier Delta Bronze défini dans config.py
    return spark.readStream.format("delta").load(BRONZE_BOOKINGS)

# --- 2. VUE DE TRANSFORMATION ---

@dlt.view(name="transform_bookings")

def transform_bookings():

    # dlt.read_stream() crée le lien de dépendance avec la vue précédente
    return dlt.read_stream("silver_flights_stage_bookings") \
        .withColumn("booking_date", to_date(col("booking_date"),"yyyy-MM-dd")) \
        .withColumn("amount", col("amount").cast("double")) \
        .withColumn("modifiedDate", current_timestamp()) \
        .drop("_rescued_data") # On supprime les données corrompues capturées par l'Auto-loader

# --- 3. TABLE DE QUALITÉ (SILVER) ---

# Définition des règles de qualité (Expectations)
rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL",
    "rule3": "flight_id IS NOT NULL"
}

@dlt.table(
    name="silver_flights_bookings",
    comment="Table Silver des réservations filtrée pour la qualité"

)

# expect_all_or_drop : Si une règle échoue, la ligne est rejetée du pipeline
@dlt.expect_all_or_drop(rules)
def silver_flights_bookings():

    # Le résultat final est une table physique Delta
    return dlt.read_stream("transform_bookings")
 