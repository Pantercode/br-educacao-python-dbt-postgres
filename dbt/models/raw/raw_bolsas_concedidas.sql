-- raw/tabela bolsas-de-cotas-concedidas.sql que esta na camada raw

{{ config(schema='raw', alias='raw_bolsas_concedidas', materialized='view') }}

SELECT *
FROM {{ ref('bolsas-de-cotas-concedidas') }}  -- o seed
