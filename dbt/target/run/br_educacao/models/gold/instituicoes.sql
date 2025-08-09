
  
    

  create  table "censo"."public_public_gold"."instituicoes__dbt_tmp"
  
  
    as
  
  (
    



WITH INSTITUICAO AS (
    SELECT
        id,
        INITCAP(CAST(instituicaoexecutora_sigla AS text)) AS sigla_instituicao,
        INITCAP(CAST(instituicaoexecutora_nome   AS text)) AS nome_instituicao,
        INITCAP(CAST(programa                  AS text)) AS programa
    from "censo"."public_public_silver"."silver_bolsas_concedidas"
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
  );
  