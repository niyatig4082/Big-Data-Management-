from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, FloatType
)
import random
import string

PEOPLE_COUNT     = 500_000   
CONNECTED_COUNT  = 1_000     
OUTPUT_DIR       = "data"    
SEED             = 42


spark = SparkSession.builder \
    .appName("CS585_P3_DataGeneration") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"Generating PEOPLE ({PEOPLE_COUNT:,} records)...")


@F.udf(StringType())
def random_name(id):
    """Deterministic fake name based on id."""
    random.seed(id)
    first = ''.join(random.choices(string.ascii_uppercase, k=1)) + \
            ''.join(random.choices(string.ascii_lowercase, k=5))
    last  = ''.join(random.choices(string.ascii_uppercase, k=1)) + \
            ''.join(random.choices(string.ascii_lowercase, k=6))
    return f"{first} {last}"

@F.udf(StringType())
def random_email(id):
    """Deterministic fake email based on id."""
    random.seed(id + 999_999)
    user   = ''.join(random.choices(string.ascii_lowercase, k=7))
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "wpi.edu"])
    return f"{user}@{domain}"



people_df = spark.range(1, PEOPLE_COUNT + 1) \
    .withColumnRenamed("id", "id") \
    .withColumn("x",     (F.rand(seed=SEED)     * 14999 + 1).cast(FloatType())) \
    .withColumn("y",     (F.rand(seed=SEED + 1) * 14999 + 1).cast(FloatType())) \
    .withColumn("name",  random_name(F.col("id").cast(IntegerType()))) \
    .withColumn("age",   (F.rand(seed=SEED + 2) * 62 + 18).cast(IntegerType())) \
    .withColumn("email", random_email(F.col("id").cast(IntegerType())))

people_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{OUTPUT_DIR}/PEOPLE")

print(f"  ✓ PEOPLE written to {OUTPUT_DIR}/PEOPLE")


print(f"Generating CONNECTED ({CONNECTED_COUNT:,} records — subset of PEOPLE)...")

random.seed(SEED)
connected_ids = random.sample(range(1, PEOPLE_COUNT + 1), CONNECTED_COUNT)

connected_ids_df = spark.createDataFrame(
    [(int(i),) for i in connected_ids],
    schema=StructType([StructField("id", IntegerType(), False)])
)

connected_df = connected_ids_df.join(
    people_df, on="id", how="inner"
)

connected_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{OUTPUT_DIR}/CONNECTED")

print(f"  ✓ CONNECTED written to {OUTPUT_DIR}/CONNECTED")



print("Generating PEOPLE_WITH_HANDSHAKE_INFO (Option 1: PEOPLE + HANDSHAKE column)...")

connected_ids_broadcast = spark.createDataFrame(
    [(int(i),) for i in connected_ids],
    schema=StructType([StructField("id", IntegerType(), False)])
).withColumn("HANDSHAKE", F.lit("yes"))

people_with_info_df = people_df.join(
    connected_ids_broadcast, on="id", how="left"
).withColumn(
    "HANDSHAKE",
    F.when(F.col("HANDSHAKE") == "yes", F.lit("yes")).otherwise(F.lit("no"))
)

people_with_info_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{OUTPUT_DIR}/PEOPLE_WITH_HANDSHAKE_INFO")

print(f"  ✓ PEOPLE_WITH_HANDSHAKE_INFO written to {OUTPUT_DIR}/PEOPLE_WITH_HANDSHAKE_INFO")


print("\n── Sanity Checks ──────────────────────────")

people_count    = people_df.count()
connected_count = connected_df.count()
yes_count = people_with_info_df.filter(F.col("HANDSHAKE") == "yes").count()
no_count  = people_with_info_df.filter(F.col("HANDSHAKE") == "no").count()

print(f"PEOPLE row count:                    {people_count:>10,}")
print(f"CONNECTED row count:                 {connected_count:>10,}")
print(f"PEOPLE_WITH_HANDSHAKE_INFO (yes):    {yes_count:>10,}")
print(f"PEOPLE_WITH_HANDSHAKE_INFO (no):     {no_count:>10,}")
print(f"yes + no == PEOPLE?                  {yes_count + no_count == people_count}")

print("\nSample rows from each dataset:")
print("PEOPLE:")
people_df.show(3, truncate=False)

print("CONNECTED:")
connected_df.show(3, truncate=False)

print("PEOPLE_WITH_HANDSHAKE_INFO:")
people_with_info_df.show(3, truncate=False)

spark.stop()
print("\nDone! All datasets generated.")