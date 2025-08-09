
  
    

  create  table "censo"."public_public_gold"."fato_bolsa__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    id,
    ano,
    id_bolsa,
    valortotalprevisto,
    data_disponibilizacao
FROM "censo"."public_public_silver"."silver_bolsas_concedidas"
WHERE 1 = 1
  );
  