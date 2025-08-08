
  
    

  create  table "censo"."public_gold"."fato_bolsas__dbt_tmp"
  
  
    as
  
  (
    SELECT
    CAST(_id AS TEXT) AS id,
    CAST(ano AS INTEGER) AS ano,
    CAST(idbolsa AS TEXT) AS id_bolsa,
    CAST(valortotalprevisto AS FLOAT) AS valortotalprevisto,
    CAST(datadisponibilizacao AS DATE) AS data_disponibilizacao
FROM "censo"."public_bronze"."bolsas-de-cotas-concedidas"
WHERE 1 = 1
  );
  