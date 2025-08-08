{{ config(MATERIALIZED='table', ALIAS='gold_bolsistas') }}

WITH bolsista AS (
    SELECT
        id_bolsa,
        bolsista
    FROM (
        SELECT
            CAST(id_bolsa AS TEXT) AS id_bolsa,
            CAST(bolsista AS TEXT) AS bolsista,
            ROW_NUMBER() OVER (
                PARTITION BY id_bolsa
                ORDER BY bolsista
            ) AS rn
        FROM {{ REF('silver_bolsas_concedidas') }}
        WHERE bolsista IS NOT NULL
          AND bolsista != ''
          AND id_bolsa IS NOT NULL
    ) t
    WHERE rn = 1
)
SELECT
    id_bolsa,
    bolsista
FROM bolsista
