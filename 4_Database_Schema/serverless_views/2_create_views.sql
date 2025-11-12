
USE RadarLakehouseDB;

-- كل امر يشتغل ف مره لوحده 

CREATE VIEW vw_fact_journeys AS
SELECT *
FROM
    OPENROWSET(
        BULK 'https://radardatalake1.dfs.core.windows.net/radarcont/clean-tables/fact_journeys/',
        FORMAT = 'PARQUET'
    ) AS [result];

GO

CREATE VIEW vw_dim_vehicles AS
SELECT *
FROM
    OPENROWSET(
        BULK 'https://radardatalake1.dfs.core.windows.net/radarcont/clean-tables/dim_vehicles/',
        FORMAT = 'PARQUET'
    ) AS [result];

GO

CREATE VIEW vw_radar_logs AS
SELECT *
FROM
    OPENROWSET(
        BULK 'https://radardatalake1.dfs.core.windows.net/radarcont/clean-tables/radar_logs/',
        FORMAT = 'PARQUET'
    ) AS [result];

GO

CREATE VIEW vw_violations AS
SELECT *
FROM
    OPENROWSET(
        BULK 'https://radardatalake1.dfs.core.windows.net/radarcont/clean-tables/violations/',
        FORMAT = 'PARQUET'
    ) AS [result];