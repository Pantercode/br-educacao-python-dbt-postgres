
  
    

  create  table "censo"."silver"."silver_bolsas_concedidas__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    CAST(_id AS TEXT)                               as id,
    CAST(ano AS INTEGER)                           as ano,
    CAST(instituicaoexecutora_sigla AS TEXT)        as instituicaoexecutora_sigla,
    CAST(instituicaoexecutora_nome  AS TEXT)        as instituicaoexecutora_nome,
    CAST(programa AS TEXT)                         as programa,
    CAST(idbolsa AS TEXT)                          as id_bolsa,
    CAST(bolsista AS TEXT)                         as bolsista,
    CAST(valortotalprevisto AS double precision) as valortotalprevisto,
    CAST(datadisponibilizacao AS DATE) data_disponibilizacao
FROM "censo"."bronze"."raw_bolsas_concedidas" 
WHERE 1 = 1
  );
  