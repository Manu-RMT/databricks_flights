####################################################################
# Silver Bookings
####################################################################
import dlt
from pyspark.sql.functions import *
from src.Communs.utils import *

src_bookings = "bookings"
bronze_path_bookings = f"{BRONZE_DATA}{src_bookings}"

@dlt.view(name="silver_flights_stage_bookings")
def silver_flights_stage_bookings():

    df_bookings = spark.readStream.format("delta").load(bronze_path_bookings)
    return df_bookings

@dlt.view(name="transform_bookings")
def transform_bookings():

    df = spark.readStream.table("silver_flights_stage_bookings")
    df = df.withColumn("booking_date", to_date(col("booking_date"),"yyyy-MM-dd")) \
           .withColumn("amount", col("amount").cast("double")) \
           .withColumn("modifiedDate", current_timestamp()) \
           .drop("_rescued_data")

    return df

rules = {
"rule1": "booking_id IS NOT NULL",
"rule2": "passenger_id IS NOT NULL",
"rule3": "flight_id IS NOT NULL"
}
 
@dlt.table(name="silver_flights_bookings")
@dlt.expect_all_or_drop(rules)
def silver_flights_bookings():
    
   return spark.readStream.table("transform_bookings")


####################################################################
# Silver Flights
####################################################################

# Utilisation de Auto CD = > pour modification / update

src_flights = "flights"
bronze_path_flights = f"{BRONZE_DATA}{src_flights}"

@dlt.view(name="silver_flights_stage_flights")
def silver_flights_stage_flights():

    df_flights = spark.readStream.format("delta").load(bronze_path_flights)
    df_flights = df_flights.drop("_rescued_data") \
                            .withColumn("modifiedDate", current_timestamp())
    return df_flights

dlt.create_streaming_table("silver_flights_flights")
dlt.create_auto_cdc_flow(
    target = "silver_flights_flights",
    source = "silver_flights_stage_flights",
    keys = ["flight_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)


####################################################################
# Silver Customers / Passengers
####################################################################

src_cust = "customers"
bronze_path_customers = f"{BRONZE_DATA}{src_cust}"

@dlt.view(name="silver_flights_stage_customers")
def silver_flights_stage_customers():

    df_cust = spark.readStream.format("delta").load(bronze_path_customers)
    df_cust = df_cust.drop("_rescued_data") \
                            .withColumn("modifiedDate", current_timestamp())
    return df_cust

dlt.create_streaming_table("silver_flights_customers")
dlt.create_auto_cdc_flow(
    target = "silver_flights_customers",
    source = "silver_flights_stage_customers",
    keys = ["passenger_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)


####################################################################
# Silver Airports
####################################################################

src_airports = "airports"
bronze_path_airports = f"{BRONZE_DATA}{src_airports}"

@dlt.view(name="silver_flights_stage_airports")
def silver_flights_stage_airports():

    df_airports = spark.readStream.format("delta").load(bronze_path_airports)
    df_airports = df_airports.drop("_rescued_data") \
                            .withColumn("modifiedDate", current_timestamp())
    return df_airports

dlt.create_streaming_table("silver_flights_airports")
dlt.create_auto_cdc_flow(
    target = "silver_flights_airports",
    source = "silver_flights_stage_airports",
    keys = ["airport_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)


####################################################################
# Silver Business View (group all transformations) - Juste pour avoir une vue globale 
####################################################################

@dlt.table(name="silver_flights_business")
def silver_flights_business():
    
    df = dlt.readStream("silver_flights_bookings") \
           .join(dlt.readStream("silver_flights_flights"), ["flight_id"]) \
           .join(dlt.readStream("silver_flights_customers"),["passenger_id"]) \
           .join(dlt.readStream("silver_flights_airports"), ["airport_id"]) \
           .drop("modifiedDate")
    
    return df
