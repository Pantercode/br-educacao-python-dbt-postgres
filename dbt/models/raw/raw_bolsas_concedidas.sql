-- raw/tabela bolsas-de-cotas-concedidas.sql que esta na camada raw

{{ config(alias='raw_bolsas_de_cotas_concedidas', materialized='view') }}

SELECT *
FROM {{ ref('bolsas-de-cotas-concedidas') }}  -- o seed
