
  create view "censo"."bronze"."raw_bolsas_concedidas__dbt_tmp"
    
    
  as (
    -- raw/tabela bolsas-de-cotas-concedidas.sql que esta na camada raw



SELECT *
FROM "censo"."bronze"."bolsas-de-cotas-concedidas"  -- o seed
  );