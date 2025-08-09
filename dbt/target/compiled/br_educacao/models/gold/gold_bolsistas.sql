

WITH BOLSISTA as (
    SELECT
        id_bolsa,
        bolsista
    FROM (
        SELECT
            CAST(id_bolsa as text) as id_bolsa,
            CAST(bolsista as text) as bolsista,
            ROW_NUMBER() OVER (
               PARTITION BY id_bolsa
            ORDER BY bolsista
            ) AS rn
        FROM "censo"."public_silver"."silver_bolsas_concedidas"
        WHERE bolsista IS NOT NULL
          AND bolsista != ''
          AND id_bolsa IS NOT NULL
    ) t
    WHERE rn = 1
)
SELECT
    id_bolsa,
    bolsista
FROM BOLSISTA