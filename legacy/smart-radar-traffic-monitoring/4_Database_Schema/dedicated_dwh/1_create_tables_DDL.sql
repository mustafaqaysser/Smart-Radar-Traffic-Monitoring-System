-- DDL: إنشاء الجداول السبعة (7)



-- 1. جدول أبعاد الطرق (ثابت)
CREATE TABLE dim_routes (
    route_id VARCHAR(50) NOT NULL PRIMARY KEY NONCLUSTERED,
    route_name VARCHAR(100),
    speed_limit INT,
    center_lat DECIMAL(10,6),
    center_lon DECIMAL(10,6),
    radar_count INT
) WITH (DISTRIBUTION = REPLICATE, CLUSTERED INDEX (route_id));

-- 2. جدول أبعاد الرادارات (ثابت)
CREATE TABLE dim_radars (
    radar_id VARCHAR(50) NOT NULL PRIMARY KEY NONCLUSTERED,
    route_id VARCHAR(50),
    radar_index INT,
    lat DECIMAL(10,6),
    lon DECIMAL(10,6)
) WITH (DISTRIBUTION = REPLICATE, CLUSTERED INDEX (radar_id));

-- 3. جدول أبعاد المركبات 
CREATE TABLE dim_vehicles (
    plate VARCHAR(20) NOT NULL PRIMARY KEY NONCLUSTERED,
    color VARCHAR(20),
    created_at DATETIME2
) WITH (DISTRIBUTION = REPLICATE, CLUSTERED INDEX (plate));

-- 4. جدول حقائق الرحلات 
CREATE TABLE fact_journeys (
    journey_id VARCHAR(50) NOT NULL PRIMARY KEY NONCLUSTERED,
    plate VARCHAR(20),
    route_id VARCHAR(50),
    driver_profile VARCHAR(20),
    start_time DATETIME2,
    end_time DATETIME2,
    total_distance DECIMAL(10,2),
    total_violations INT DEFAULT 0,
    total_fines INT DEFAULT 0
) WITH (DISTRIBUTION = HASH(journey_id), CLUSTERED INDEX (journey_id));

-- 5. جدول سجلات الرادار 
CREATE TABLE radar_logs (
    id INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED, 
    journey_id VARCHAR(50),
    plate VARCHAR(20),
    speed INT,
    speed_limit INT,
    color VARCHAR(20),
    radar_id VARCHAR(20),
    radar_index INT,
    lat DECIMAL(10,6),
    lon DECIMAL(10,6),
    seat_belt BIT,
    phone_usage BIT,
    is_violation BIT,
    violation_codes VARCHAR(200),
    total_fine INT,
    segment_distance_km DECIMAL(8,4),
    timestamp DATETIME2 
) WITH (DISTRIBUTION = HASH(journey_id), CLUSTERED COLUMNSTORE INDEX);

-- 6. جدول المخالفات 

CREATE TABLE violations (
    id INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED, 
    journey_id VARCHAR(50),
    plate VARCHAR(20),
    reason VARCHAR(100),
    fine INT,
    timestamp DATETIME2 
) WITH (DISTRIBUTION = HASH(journey_id), CLUSTERED COLUMNSTORE INDEX);

-- 7. جدول سجلات النظام 

CREATE TABLE system_logs (
    log_id INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED, 
    log_level VARCHAR(20),
    message NVARCHAR(MAX), 
    details NVARCHAR(MAX), 
    timestamp DATETIME2 DEFAULT GETUTCDATE()
) WITH (DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX);