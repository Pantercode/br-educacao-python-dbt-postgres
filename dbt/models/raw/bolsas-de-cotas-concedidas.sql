-- raw/tabela bolsas-de-cotas-concedidas.sql que esta na camada raw

SELECT *
FROM {{ ref('bolsas-de-cotas-concedidas') }}
