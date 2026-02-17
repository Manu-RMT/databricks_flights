# lib/config.py

# ----------------------
# Catalog & Schemas
# ----------------------
CATALOG = "workspace"        # Nom du catalog Unity
RAW_SCHEMA = "raw_flights"          # Schema temporaire pour RAW
BRONZE_SCHEMA = "01_bronze"     # Schema pour Bronze (raw)
SILVER_SCHEMA = "02_silver"     # Schema pour Silver (Delta nettoyé)
GOLD_SCHEMA = "03_gold"         # Schema pour Gold (BI-ready)

# ----------------------
# Volume & fichiers
# ----------------------
VOLUME_RAW_NAME = "raw_flights/raw_data"
VOLUME_PATH = "/Volumes/workspace/"+ VOLUME_RAW_NAME + "/"  # Volume contenant tous les CSV
VOLUME_NAME= CATALOG + "." + RAW_SCHEMA
CSV_EXTENSION = ".csv"                             # Extension des fichiers à lire


# ----------------------
# Diffentes couches RAW
# ----------------------
RAW_BRONZE_ZONE = f"{CATALOG}.{RAW_SCHEMA}.{BRONZE_SCHEMA}"
RAW_SILVER_ZONE = f"{CATALOG}.{RAW_SCHEMA}.{SILVER_SCHEMA}"
RAW_GOLD_ZONE = f"{CATALOG}.{RAW_SCHEMA}.{GOLD_SCHEMA}"

# ----------------------
# Diffentes couches 
# ----------------------
BRONZE_ZONE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER_ZONE = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD_ZONE = f"{CATALOG}.{GOLD_SCHEMA}"

