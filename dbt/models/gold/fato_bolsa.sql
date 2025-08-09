{{ config(schema='public_gold',materialized='table', alias='fato_bolsa') }}

SELECT
    id,
    ano,
    id_bolsa,
    valortotalprevisto,
    data_disponibilizacao
FROM {{ ref('silver_bolsas_concedidas') }}
WHERE 1 = 1
