from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, avg

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Data Mapping Example") \
    .getOrCreate()

# Read CSV files
employees_df = spark.read.csv("employee.csv", header=True, inferSchema=True)
departments_df = spark.read.csv("department.csv", header=True, inferSchema=True)

# Show the original dataframes
print("Employees Data:")
employees_df.show()

print("Departments Data:")
departments_df.show()

# Example 1: Simple Join Operation
print("Example 1: Employee details with department information")
result1 = employees_df.join(
    departments_df.select(
        col("dept_id").alias("deprtnt_dept_id"),
        col("dept_name"),
        col("location")
    ),
    employees_df.dept_id == col("deprtnt_dept_id"),
    "inner"
)

result1.select(
    "emp_id",
    "emp_name",
    "salary",
    "dept_id",
    "deprtnt_dept_id",
    "dept_name",
    "location"
).show()

# Example 2: Aggregation by Department
print("Example 2: Average salary by department")
result2 = result1.groupBy("dept_name", "deprtnt_dept_id") \
    .agg(
        round(avg("salary"), 2).alias("avg_salary")
    ) \
    .orderBy("dept_name")
result2.show()

# Example 3: Department-wise Employee Count

print("Example 3: Number of employees in each department")
result3 = result1.groupBy("deprtnt_dept_id","dept_name", "location") \
    .count() \
    .orderBy("deprtnt_dept_id")
result3.show()

# Stop Spark Session
spark.stop()
