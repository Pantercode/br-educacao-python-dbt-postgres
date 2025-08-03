-- Cria uma VISÃO com escolas (nome e UF), deduplicada pelo código
-- Funciona em Postgres antigo (evita funções novas)

with base as (
    select
        nullif(trim(codigo_escola), '')::bigint as id_escola,
        nullif(trim(nome_escola), '')           as nome_escola,
        upper(nullif(trim(sigla_uf), ''))       as sigla_uf,
        fetched_at
    from public.api_ideb_escolas
    where codigo_escola is not null
), ordenado as (
    select
        *,
        row_number() over (partition by id_escola order by fetched_at desc) as rn
    from base
)
select id_escola, nome_escola, sigla_uf, fetched_at
from ordenado
where rn = 1;