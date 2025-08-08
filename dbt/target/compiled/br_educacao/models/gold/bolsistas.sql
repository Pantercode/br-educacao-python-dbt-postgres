WITH BOLSISTAS AS (
    SELECT 
        CAST(idbolsa AS TEXT) AS id_bolsa,
        CAST(bolsista AS TEXT) AS bolsista,
        ROW_NUMBER() OVER (PARTITION BY idbolsa ORDER BY bolsista) AS row_num
    FROM "censo"."public_bronze"."bolsas-de-cotas-concedidas"
    WHERE 1 = 1
    AND bolsista IS NOT NULL
    AND bolsista != ''
    AND idbolsa IS NOT NULL
)
SELECT
    id_bolsa,
    bolsista
FROM BOLSISTAS
WHERE row_num = 1;