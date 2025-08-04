{{ config(materialized='view') }}
select now()::timestamp as gerado_em, t.table_schema, t.table_name, t.table_type
from information_schema.tables t
where t.table_schema = 'gold'
order by t.table_type, t.table_name;
