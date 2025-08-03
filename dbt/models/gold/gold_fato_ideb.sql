-- Exemplo simples de FATO a partir do resumo por UF (ajuste conforme colunas reais)

select
    upper(nullif(trim(sigla_uf), '')) as sigla_uf,
    cast(fetched_at as timestamp)     as data_carga,
    -- as colunas abaixo são exemplos; renomeie conforme o que existir na tabela
    nullif(trim(ano), '')::int        as ano,
    nullif(trim(ideb), '')::numeric   as ideb_valor
from public.api_ideb_resumo_uf;