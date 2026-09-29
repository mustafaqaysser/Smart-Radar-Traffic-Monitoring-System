# daily_elt_job.py
# ----------------------------
# (النسخة الأصلية - DWH)
# مهمته: قراءة البيانات الخام (JSON) من Data Lake، وتحويلها،
# وحفظها مباشرة في جداول Synapse Dedicated SQL Pool (DWH).
# ----------------------------

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, lit, explode, split, 
    min, max, sum, count, first, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType
)

# ----------------------------
# ⚠️ 1. إعدادات الاتصال 
# ----------------------------

# 1. إعدادات الاتصال بـ Synapse DWH (الـ Dedicated SQL Pool)
# (استبدل القيم!)
SYNAPSE_SERVER = "radar-synapse-workspace.sql.azuresynapse.net"  # ⬅️ (اسم السيرفر)
SYNAPSE_DB = "RadarDW"  # ⬅️ (الاسم الذي اخترته للـ SQL Pool)
SYNAPSE_USER = "sqladminuser"  # ⬅️(اليوزر الخاص بـ SQL)
SYNAPSE_PASS = "YourPassword!"  # ⬅️ (كلمة المرور)

# هذا السطر يبني "عنوان" الاتصال
SYNAPSE_SQL_URL = f"jdbc:sqlserver://{SYNAPSE_SERVER}:1433;database={SYNAPSE_DB};user={SYNAPSE_USER};password={SYNAPSE_PASS}"
SYNAPSE_TABLE_OPTIONS = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# 2. مسار البيانات الخام (JSON)
# (هذا المسار يقرأ ملفات .json فقط، كما اتفقنا)
DATA_LAKE_RAW_PATH = "abfss://radarcont@radardatalake1.dfs.core.windows.net/*.json"

# ----------------------------
# بيانات ثابتة (لترجمة المخالفات)
# ----------------------------
VIOLATION_TYPES_DATA = [
    ("SPD_001", "Moderate Speeding", 500),
    ("SPD_002", "Severe Speeding", 1000),
    ("SBL_001", "No Seat Belt", 300),
    ("PHN_001", "Phone Usage", 400),
]

# ----------------------------
# دوال مساعدة
# ----------------------------

def get_spark_session():
    """إنشاء أو جلب Spark Session."""
    return SparkSession.builder \
        .appName("Radar Batch ELT Job (DWH)") \
        .getOrCreate()

def save_to_synapse(df, table_name, mode="append"):
    """
    دالة لحفظ DataFrame في Synapse DWH باستخدام JDBC.
    """
    print(f"🚀 بدء الكتابة في جدول DWH: {table_name}, الوضع: {mode}")
    try:
        df.write \
          .format("jdbc") \
          .options(**SYNAPSE_TABLE_OPTIONS) \
          .option("url", SYNAPSE_SQL_URL) \
          .option("dbtable", table_name) \
          .mode(mode) \
          .save()
        print(f"✅ تمت الكتابة بنجاح في: {table_name}")
    except Exception as e:
        print(f"❌ فشل في الكتابة لـ {table_name}: {e}")

# ----------------------------
# منطق الـ ELT (الوظائف الرئيسية)
# ----------------------------

def load_raw_data(spark, input_path):
    """
    1. قراءة بيانات JSON الخام من Data Lake.
    """
    print(f"📥 قراءة البيانات الخام من: {input_path}")
    
    raw_schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("journey_id", StringType(), True),
        StructField("plate", StringType(), True),
        StructField("color", StringType(), True),
        StructField("driver_profile", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("radar_id", StringType(), True),
        StructField("radar_index", IntegerType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("speed", IntegerType(), True),
        StructField("speed_limit", IntegerType(), True),
        StructField("seat_belt", BooleanType(), True),
        StructField("phone_usage", BooleanType(), True),
        StructField("is_violation", BooleanType(), True),
        StructField("violation_codes", StringType(), True),
        StructField("total_fine", IntegerType(), True),
        StructField("segment_distance_km", DoubleType(), True)
    ])

    df = spark.read.format("json") \
             .schema(raw_schema) \
             .load(input_path)
    
    df = df.withColumn("timestamp_dt", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")) \
           .dropDuplicates(["id"])
           
    df.cache() 
    count = df.count()
    print(f"📊 تم قراءة {count} سجل خام.")
    return df

def process_logs_and_violations(spark, df_raw):
    """
    2. معالجة وحفظ جداول السجلات (Logs & Violations).
    (نستخدم "append" هنا لأننا نريد فقط إضافة السجلات الجديدة كل يوم)
    """
    print(" processing logs and violations (Mode: append)...")

    # --- radar_logs ---
    df_radar_logs = df_raw.select(
        col("id"), 
        col("journey_id"), "plate", "speed", "speed_limit", "color", "radar_id",
        "radar_index", "lat", "lon", "seat_belt", "phone_usage", "is_violation",
        "violation_codes", "total_fine", "segment_distance_km",
        col("timestamp_dt").alias("timestamp")
    )
  
    save_to_synapse(df_radar_logs, "radar_logs", mode="append")

    # --- violations ---
    violation_schema = StructType([
        StructField("reason_code", StringType()),
        StructField("reason_name", StringType()),
        StructField("fine_amount", IntegerType())
    ])
    df_violation_lookup = spark.createDataFrame(VIOLATION_TYPES_DATA, violation_schema)

    df_violations_raw = df_raw.filter(col("is_violation") == True) \
                              .filter(col("violation_codes").isNotNull())
    
    df_violations_exploded = df_violations_raw.select(
        col("journey_id"),
        col("plate"),
        col("timestamp_dt").alias("timestamp"),
        explode(split(col("violation_codes"), ";")).alias("reason_code")
    )
    
    df_violations_final = df_violations_exploded.join(
        df_violation_lookup,
        df_violations_exploded.reason_code == df_violation_lookup.reason_code,
        "left"
    ).select(
        col("journey_id"),
        col("plate"),
        col("timestamp"),
        col("reason_name").alias("reason"), 
        col("fine_amount").alias("fine")      
    )
    
    save_to_synapse(df_violations_final, "violations", mode="append")

def process_dims_and_facts(df_raw):
    """
    3. معالجة وحفظ جداول الأبعاد والحقائق (Dims & Facts).
    (نستخدم "overwrite" هنا للتبسيط)
    """
    print(" processing dims and facts (Mode: overwrite)...")

    # --- dim_vehicles ---
    df_vehicles = df_raw.select("plate", "color", "timestamp_dt") \
                        .groupBy("plate") \
                        .agg(
                            first("color").alias("color"),
                            min("timestamp_dt").alias("created_at")
                        )
    
    save_to_synapse(df_vehicles, "dim_vehicles", mode="overwrite")

    # --- fact_journeys ---
    df_facts = df_raw.groupBy("journey_id") \
                     .agg(
                         first("plate").alias("plate"),
                         first("route_id").alias("route_id"),
                         first("driver_profile").alias("driver_profile"),
                         min("timestamp_dt").alias("start_time"),
                         max("timestamp_dt").alias("end_time"),
                         sum("segment_distance_km").alias("total_distance"),
                         sum(when(col("is_violation") == True, 1).otherwise(0)).alias("total_violations"),
                         sum("total_fine").alias("total_fines")
                     )
    
    save_to_synapse(df_facts, "fact_journeys", mode="overwrite")

# ----------------------------
# Main execution
# ----------------------------
def main():
    try:
        spark
    except NameError:
        spark = get_spark_session()
    
    df_raw = load_raw_data(spark, DATA_LAKE_RAW_PATH)
    
    process_logs_and_violations(spark, df_raw)
    
    process_dims_and_facts(df_raw)
    
    print("✅ ELT Batch Job (DWH) Completed Successfully.")

if __name__ == "__main__":
    main()