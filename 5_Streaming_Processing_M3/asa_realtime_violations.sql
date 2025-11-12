/*
-- هذا هو الاستعلام الخاص بوظيفة Stream Analytics (M3)
-- الهدف: اكتشاف المخالفات لحظيًا وإرسالها إلى Power BI
*/

SELECT
    plate,
    speed,
    speed_limit,
    lat,
    lon,
    violation_codes,
    total_fine,
    timestamp
INTO
    [powerbi_output]  -- (اسم المخرج "Output" الذي تم ضبطه لـ Power BI)
FROM
    [eventhub_input]  -- (اسم المدخل "Input" المرتبط بـ Event Hub)
WHERE
    is_violation = true