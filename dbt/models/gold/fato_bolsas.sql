{{ config(materialized='table', alias='gold__minha_tabela') }}

SELECT
    id,
    ano,
    id_bolsa,
    valortotalprevisto,
    data_disponibilizacao
FROM {{ ref('silver_bolsas_concedidas') }}
WHERE 1 = 1
