
  
    

  create  table "censo"."public_silver"."silver-bolsas-de-cotas-concedidas__dbt_tmp"
  
  
    as
  
  (
    SELECT
    CAST(_id AS TEXT) AS id,
    CAST(ano AS INTEGER) AS ano,
    CAST(instituicaoexecutor_sigla AS TEXT) AS instituicaoexecutor_sigla,
    CAST(instituicaoexecutor_nome AS TEXT) AS instituicaoexecutor_sigla,
    CAST(programa AS TEXT) AS programa,
    CAST(idbolsa AS TEXT) AS id_bolsa,
    CAST(bolsista AS TEXT) AS bolsista,
    CAST(valortotalprevisto AS FLOAT) AS valortotalprevisto,
    CAST(datadisponibilizacao AS DATE) AS data_disponibilizacao
FROM "censo"."public_bronze"."bolsas-de-cotas-concedidas"
WHERE 1 = 1
  );
  