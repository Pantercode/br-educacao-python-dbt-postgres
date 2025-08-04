{{ config(materialized='view') }}
select now()::timestamp as gerado_em, t.table_schema, t.table_name
from information_schema.tables t
where t.table_schema = 'silver' and t.table_type = 'BASE TABLE'
order by t.table_name;
