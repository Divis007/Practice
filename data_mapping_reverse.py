from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, substring, to_date, current_date, datediff, lit, concat, lpad, lower, when

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Reverse Engineering Data Mapping") \
    .getOrCreate()

# Read both CSV files
df_v1 = spark.read.csv("employee.csv", header=True, inferSchema=True)
df_v2 = spark.read.csv("employee_v2.csv", header=True, inferSchema=True)

# Show original dataframes
print("Original Employee Data (V1):")
df_v1.show()

print("\nNew Employee Data (V2):")
df_v2.show()

# 1. Mapping V2 to V1 format
print("\nExample 1: Converting V2 format to V1 format")
v2_to_v1 = df_v2.select(
    # Extract numeric part from employee_number and cast to integer
    regexp_replace('employee_number', 'EMP', '').cast('integer').alias('emp_id'),
    col('full_name').alias('emp_name'),
    # Extract numeric part from department_code
    regexp_replace('department_code', 'DEPT', '').cast('integer').alias('dept_id'),
    col('annual_compensation').alias('salary')
)

print("Mapped V2 to V1 format:")
v2_to_v1.show()

# 2. Mapping V1 to V2 format
print("\nExample 2: Converting V1 format to V2 format")
v1_to_v2 = df_v1.select(
    # Add 'EMP' prefix to emp_id
    concat(lit('EMP'), lpad(col('emp_id').cast('string'), 3, '0')).alias('employee_number'),
    col('emp_name').alias('full_name'),
    # Add 'DEPT' prefix to dept_id
    concat(lit('DEPT'), col('dept_id').cast('string')).alias('department_code'),
    col('salary').alias('annual_compensation'),
    # Generate sample hire date (for demonstration)
    current_date().alias('hire_date'),
    # Sample job title based on department
    when(col('dept_id') == 101, 'Engineer')
        .when(col('dept_id') == 102, 'Marketing Specialist')
        .when(col('dept_id') == 103, 'Financial Analyst')
        .otherwise('Staff Member').alias('job_title'),
    # Generate sample email
    lower(concat(
        regexp_replace(col('emp_name'), ' ', '.'),
        lit('@company.com')
    )).alias('email')
)

print("Mapped V1 to V2 format:")
v1_to_v2.show()

# 3. Schema Comparison
print("\nExample 3: Schema Comparison")
print("\nV1 Schema:")
df_v1.printSchema()
print("\nV2 Schema:")
df_v2.printSchema()

# 4. Data Quality Check
print("\nExample 4: Data Quality Check")
# Check for null values in both datasets
print("\nNull values in V1:")
for column in df_v1.columns:
    null_count = df_v1.filter(col(column).isNull()).count()
    print(f"{column}: {null_count} null values")

print("\nNull values in V2:")
for column in df_v2.columns:
    null_count = df_v2.filter(col(column).isNull()).count()
    print(f"{column}: {null_count} null values")

# 5. Field Value Distribution
print("\nExample 5: Department Distribution Comparison")
print("\nV1 Department Distribution:")
df_v1.groupBy('dept_id').count().orderBy('dept_id').show()

print("\nV2 Department Distribution:")
df_v2.groupBy('department_code').count().orderBy('department_code').show()

# Stop Spark Session
spark.stop()
