

select
    CAST(id AS TEXT)                               as id,
    CAST(ano AS INTEGER)                           as ano,
    CAST(instituicaoexecutor_sigla AS TEXT)        as instituicaoexecutor_sigla,
    CAST(instituicaoexecutor_nome  AS TEXT)        as instituicaoexecutor_nome,
    CAST(programa AS TEXT)                         as programa,
    CAST(idbolsa AS TEXT)                          as id_bolsa,
    CAST(bolsista AS TEXT)                         as bolsista,
    CAST(valortotalprevisto AS DOUBLE PRECISION)   as valor_total_previsto,
    coalesce(
        try_cast(data_disponibilizacao AS DATE),
        try_cast(datadisponibilizacao  AS DATE)
    )                                              as data_disponibilizacao
FROM "censo"."public_bronze"."raw_bolsas_concedidas" -- Remova o '.sql' daqui
WHERE 1 = 1