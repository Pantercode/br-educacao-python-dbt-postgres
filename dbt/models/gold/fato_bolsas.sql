SELECT
    CAST(_id AS TEXT) AS id,
    CAST(ano AS INTEGER) AS ano,
    CAST(idbolsa AS TEXT) AS id_bolsa,
    CAST(bolsista AS TEXT) AS bolsista,
    CAST(valortotalprevisto AS FLOAT) AS valortotalprevisto,
    CAST(datadisponibilizacao AS DATE) AS data_disponibilizacao
FROM {{ ref('bolsas-de-cotas-concedidas') }}
WHERE 1 = 1