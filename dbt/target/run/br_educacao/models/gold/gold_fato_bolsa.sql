
  
    

  create  table "censo"."gold"."gold_fato_bolsa__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    id,
    ano,
    id_bolsa,
    valortotalprevisto,
    data_disponibilizacao
FROM "censo"."silver"."silver_bolsas_concedidas"
WHERE 1 = 1
  );
  