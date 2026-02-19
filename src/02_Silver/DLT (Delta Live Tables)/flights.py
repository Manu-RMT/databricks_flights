import dlt
from pyspark.sql.functions import *
from src.Communs.utils import *


# Utilisation de Auto CD = > pour modification / update

src_flights = "flights"
bronze_path_flightss = f"{BRONZE_DATA}{src_flightss}"

@dlt.view(name="silver_flights_stage_flightss")
def silver_flights_stage_flightss():

    df_flights = spark.readStream.format("delta").load(bronze_path_flightss)
    return df_flights

dlt.create_stream_table("silver_flights_flights")
dlt.create_auto_cdc_flow(
    target="silver_flights_flights",
    source="silver_flights_stage_flightss",
    keys=["flight_id"],
    stored_as_scd_type=1
)


