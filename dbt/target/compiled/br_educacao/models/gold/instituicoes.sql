WITH INSTITUICAO AS (
    SELECT
        CAST(_id AS TEXT) AS id,
        INITCAP(CAST(instituicaoexecutor_sigla AS TEXT)) AS sigla_instituicao,
        INITCAP(CAST(instituicaoexecutor_nome AS TEXT)) AS nome_instituicao,
        INITCAP(CAST(programa AS TEXT)) AS programa,
        ROW_NUMBER() OVER (
            PARTITION BY _id
            ORDER BY COALESCE(instituicaoexecutor_sigla, instituicaoexecutor_nome, programa) DESC
        ) AS row_num
    FROM "censo"."public_bronze"."bolsas-de-cotas-concedidas"
    WHERE 1 = 1
)
SELECT *
FROM INSTITUICAO
WHERE row_num = 1;