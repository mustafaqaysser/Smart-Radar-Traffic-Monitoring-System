# ( Optional code )
# الكود دا كان مؤقت بيحفظ الداتا الي اتعملها ترانفورميشنز ف datalakehouse
# ولاكن البروجكت مصمم انو يخزن الداتا ف data warehouse
# عشان كدا الكود دا مش هيستخدم ف النسخة النهائية من البروج
# --------------------------------------------------------------

# daily_elt_job_LAKEHOUSE.py
# ----------------------------
# (النسخة النهائية - مع إصلاح مسار القراءة)
# مهمته: قراءة البيانات الخام (JSON) من Data Lake،
# وتحويلها، وحفظها كملفات Parquet نظيفة في مجلد آخر بالـ Data Lake.
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
# ⚠️ 1. إعدادات المسار 
# ----------------------------

# 1. مسار البيانات الخام (JSON)
# (هذا هو المسار حيث يحفظ Stream Analytics الملفات)
DATA_LAKE_RAW_PATH = "abfss://radarcont@radardatalake1.dfs.core.windows.net/*.json"

# 2. مسار البيانات النظيفة (Parquet)
# (هذا هو "المستودع" الجديد المجاني. سننشئ مجلدًا جديدًا اسمه clean-tables)
DATA_LAKE_CLEAN_PATH = "abfss://radarcont@radardatalake1.dfs.core.windows.net/clean-tables/"


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
        .appName("Radar Batch ELT Job (Lakehouse)") \
        .getOrCreate()


# ----------------------------
# منطق الـ ELT (الوظائف الرئيسية)
# ----------------------------

def load_raw_data(spark, input_path):
    """
    1. قراءة بيانات JSON الخام من Data Lake.
    """
    print(f"📥 قراءة البيانات الخام من: {input_path}")
    
    # هذا المخطط يجب أن يطابق تمامًا الـ JSON الذي يرسله المنتج
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
    
    # تنظيف أساسي
    df = df.withColumn("timestamp_dt", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")) \
           .dropDuplicates(["id"])
           
    df.cache() 
    count = df.count()
    print(f"📊 تم قراءة {count} سجل خام.")
    return df

def process_logs_and_violations(spark, df_raw):
    """
    2. معالجة وحفظ جداول السجلات (Logs & Violations) كملفات Parquet.
    (نستخدم "overwrite" للتبسيط في المشروع التعليمي)
    """
    print(" processing logs and violations (Mode: overwrite)...")

    # --- radar_logs ---
    df_radar_logs = df_raw.select(
        col("id"), 
        col("journey_id"), "plate", "speed", "speed_limit", "color", "radar_id",
        "radar_index", "lat", "lon", "seat_belt", "phone_usage", "is_violation",
        "violation_codes", "total_fine", "segment_distance_km",
        col("timestamp_dt").alias("timestamp")
    )
    df_radar_logs.write.mode("overwrite").parquet(DATA_LAKE_CLEAN_PATH + "radar_logs")
    print(f"✅ تم حفظ radar_logs في: {DATA_LAKE_CLEAN_PATH}radar_logs")

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
    
    df_violations_final.write.mode("overwrite").parquet(DATA_LAKE_CLEAN_PATH + "violations")
    print(f"✅ تم حفظ violations في: {DATA_LAKE_CLEAN_PATH}violations")

def process_dims_and_facts(df_raw):
    """
    3. معالجة وحفظ جداول الأبعاد والحقائق (Dims & Facts) كملفات Parquet.
    (نستخدم "overwrite" للتبسيط)
    """
    print(" processing dims and facts (Mode: overwrite)...")

    # --- dim_vehicles ---
    df_vehicles = df_raw.select("plate", "color", "timestamp_dt") \
                        .groupBy("plate") \
                        .agg(
                            first("color").alias("color"),
                            min("timestamp_dt").alias("created_at")
                        )
    
    df_vehicles.write.mode("overwrite").parquet(DATA_LAKE_CLEAN_PATH + "dim_vehicles")
    print(f"✅ تم حفظ dim_vehicles في: {DATA_LAKE_CLEAN_PATH}dim_vehicles")

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
    
    df_facts.write.mode("overwrite").parquet(DATA_LAKE_CLEAN_PATH + "fact_journeys")
    print(f"✅ تم حفظ fact_journeys في: {DATA_LAKE_CLEAN_PATH}fact_journeys")

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
    
    print("✅ ELT Batch Job (Lakehouse) Completed Successfully.")
    
    
    spark.stop()

if __name__ == "__main__":
    main()