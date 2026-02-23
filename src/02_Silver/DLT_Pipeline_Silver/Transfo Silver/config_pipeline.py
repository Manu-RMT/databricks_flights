import dlt
import sys
# -----------------------------
# Ajouter le repo au Python Path
# -----------------------------
sys.path.append("/Workspace/Users/mandu543@gmail.com/databricks_flights")
from src.Communs.utils import *

# Chemin vers le répertoire contenant les fichiers CSV au format Parquet
BRONZE_BOOKINGS = f"{BRONZE_DATA}bookings"
BRONZE_FLIGHTS = f"{BRONZE_DATA}flights"
BRONZE_AIRPORTS = f"{BRONZE_DATA}airports"
BRONZE_CUSTOMERS = f"{BRONZE_DATA}customers"


# Le mode "Append-only" (code Bookings)
# • Comportement : Chaque nouvelle ligne arrivant en Bronze est ajoutée à la suite dans Silver.
# • Doublons : Si le même booking_id arrive deux fois avec des montants différents, tu auras deux lignes dans ta table finale.
# • Usage : Idéal pour les faits (transactions, logs, événements) où l'historique complet compte et où les données ne changent jamais une fois écrites.

# Le mode "CDC / APPLY CHANGES INTO" (fichiers Flights/Customers/Airports)
# • Comportement : DLT cherche la clé primaire (ex: flight_id). Si elle existe déjà, il met à jour la ligne. Si elle n'existe pas, il l'insère.
# • Gestion des versions : C'est là que sequence_by intervient. Si deux mises à jour arrivent, DLT utilise cette colonne pour garder la plus récente.
# • Usage : Idéal pour les dimensions ou les entités dont l'état change (un vol qui change d'heure, un client qui change d'adresse).