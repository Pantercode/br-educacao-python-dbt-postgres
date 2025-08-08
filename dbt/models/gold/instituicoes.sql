{{ config(materialized='table', alias='gold_instituicoes') }}

WITH INSTITUICAO AS (
    SELECT
        id,
        INITCAP(CAST(instituicaoexecutor_sigla AS TEXT)) AS sigla_instituicao,
        INITCAP(CAST(instituicaoexecutor_nome  AS TEXT)) AS nome_instituicao,
        INITCAP(CAST(programa                  AS TEXT)) AS programa
    FROM {{ REF('silver_bolsas_concedidas') }}
),
DEDUP AS (
    SELECT
        id,
        sigla_instituicao,
        nome_instituicao,
        programa,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY COALESCE(sigla_instituicao, nome_instituicao, programa) DESC
        ) AS rn
    FROM INSTITUICAO
    WHERE id IS NOT NULL
)
SELECT
    id,
    sigla_instituicao,
    nome_instituicao,
    programa
FROM DEDUP
WHERE rn = 1
