-- DML: تعبئة الأبعاد الثابتة (Routes & Radars)
-- يتم تشغيل هذا السكربت مرة واحدة فقط بعد إنشاء الجداول

-- 1. تعبئة الطرق (Routes)
INSERT INTO dim_routes (route_id, route_name, speed_limit, center_lat, center_lon, radar_count) VALUES
    ('Route_1', 'Cairo-Alex Road', 120, 30.0444, 31.2357, 6),
    ('Route_2', 'Ring Road East', 100, 30.0500, 31.3000, 5),
    ('Route_3', 'Corniche Road', 80, 31.2000, 29.9167, 7),
    ('Route_4', 'Downtown Circuit', 60, 30.0333, 31.2333, 4),
    ('Route_5', 'Airport Road', 100, 30.1210, 31.4050, 5);

-- 2. تعبئة الرادارات (Radars)
-- (تم حساب هذه الإحداثيات بناءً على الكود اللوجيستي في الاسكربت القديم)
INSERT INTO dim_radars (radar_id, route_id, radar_index, lat, lon) VALUES
    ('Route_1_Radar_1', 'Route_1', 1, 30.014400, 31.205700),
    ('Route_1_Radar_2', 'Route_1', 2, 30.026400, 31.217700),
    ('Route_1_Radar_3', 'Route_1', 3, 30.038400, 31.229700),
    ('Route_1_Radar_4', 'Route_1', 4, 30.050400, 31.241700),
    ('Route_1_Radar_5', 'Route_1', 5, 30.062400, 31.253700),
    ('Route_1_Radar_6', 'Route_1', 6, 30.074400, 31.265700),
    ('Route_2_Radar_1', 'Route_2', 1, 30.020000, 31.270000),
    ('Route_2_Radar_2', 'Route_2', 2, 30.035000, 31.285000),
    ('Route_2_Radar_3', 'Route_2', 3, 30.050000, 31.300000),
    ('Route_2_Radar_4', 'Route_2', 4, 30.065000, 31.315000),
    ('Route_2_Radar_5', 'Route_2', 5, 30.080000, 31.330000),
    ('Route_3_Radar_1', 'Route_3', 1, 31.170000, 29.886700),
    ('Route_3_Radar_2', 'Route_3', 2, 31.180000, 29.896700),
    ('Route_3_Radar_3', 'Route_3', 3, 31.190000, 29.906700),
    ('Route_3_Radar_4', 'Route_3', 4, 31.200000, 29.916700),
    ('Route_3_Radar_5', 'Route_3', 5, 31.210000, 29.926700),
    ('Route_3_Radar_6', 'Route_3', 6, 31.220000, 29.936700),
    ('Route_3_Radar_7', 'Route_3', 7, 31.230000, 29.946700),
    ('Route_4_Radar_1', 'Route_4', 1, 30.003300, 31.203300),
    ('Route_4_Radar_2', 'Route_4', 2, 30.023300, 31.223300),
    ('Route_4_Radar_3', 'Route_4', 3, 30.043300, 31.243300),
    ('Route_4_Radar_4', 'Route_4', 4, 30.063300, 31.263300),
    ('Route_5_Radar_1', 'Route_5', 1, 30.091000, 31.375000),
    ('Route_5_Radar_2', 'Route_5', 2, 30.106000, 31.390000),
    ('Route_5_Radar_3', 'Route_5', 3, 30.121000, 31.405000),
    ('Route_5_Radar_4', 'Route_5', 4, 30.136000, 31.420000),
    ('Route_5_Radar_5', 'Route_5', 5, 30.151000, 31.435000);