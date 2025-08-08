{{ config(materialized='table', alias='gold_instituicoes') }}

WITH INSTITUICAO AS (
    SELECT
        id,
        INITICAP(CAST(instituicaoexecutor_sigla AS text)) AS sigla_instituicao,
        INITICAP(CAST(instituicaoexecutor_nome  AS text)) AS nome_instituicao,
        INITICAP(CAST(programa                  AS text)) AS programa
    from {{ ref('silver_bolsas_concedidas') }}
),
DEDUP AS (
    SELECT
        id,
        sigla_instituicao,
        nome_instituicao,
        programa,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY coalesce(sigla_instituicao, nome_instituicao, programa) DESC
        ) AS rn
    FROM INSTITUICAO
    WHERE id is not null
)
SELECT
    id,
    sigla_instituicao,
    nome_instituicao,
    programa
FROM DEDUP
WHERE rn = 1
