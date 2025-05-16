from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Read Transactions") \
    .getOrCreate()

# Read the CSV file with inferSchema
transactions_df = spark.read.csv("transactions.csv", header=True, inferSchema=True)

# Show the data and schema
print("Transactions Data:")
transactions_df.show()
print("\nSchema:")
transactions_df.printSchema()

# Stop Spark Session
spark.stop()
