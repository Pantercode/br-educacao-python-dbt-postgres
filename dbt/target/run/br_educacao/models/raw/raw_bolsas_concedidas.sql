
  create view "censo"."public_raw"."raw_bolsas_concedidas__dbt_tmp"
    
    
  as (
    -- raw/tabela bolsas-de-cotas-concedidas.sql que esta na camada raw



SELECT *
FROM "censo"."public_bronze"."bolsas-de-cotas-concedidas"  -- o seed
  );