WITH LIMITES AS (
  SELECT
    MIN(CAST(datadisponibilizacao AS DATE)) AS data_min,
    MAX(CAST(datadisponibilizacao AS DATE)) AS data_max
  FROM {{ ref('bolsas-de-cotas-concedidas') }}
),

DATAS AS (
  SELECT generate_series(data_min, data_max, interval '1 day')::DATE AS data
  FROM LIMITES
),

NOME_MESES AS (
  SELECT * FROM (VALUES
    (1, 'Janeiro'), (2, 'Fevereiro'), (3, 'Março'), (4, 'Abril'),
    (5, 'Maio'), (6, 'Junho'), (7, 'Julho'), (8, 'Agosto'),
    (9, 'Setembro'), (10, 'Outubro'), (11, 'Novembro'), (12, 'Dezembro')
  ) AS t(numero_mes, nome_mes)
),

CALENDARIO AS (
  SELECT
    d.data,
    EXTRACT(DAY FROM d.data) AS dia,
    LPAD(EXTRACT(MONTH FROM d.data)::TEXT, 2, '0') AS numero_mes,
    m.nome_mes,
    EXTRACT(YEAR FROM d.data) AS ano,

    CASE EXTRACT(DOW FROM d.data)
      WHEN 0 THEN 'Domingo'
      WHEN 1 THEN 'Segunda-feira'
      WHEN 2 THEN 'Terça-feira'
      WHEN 3 THEN 'Quarta-feira'
      WHEN 4 THEN 'Quinta-feira'
      WHEN 5 THEN 'Sexta-feira'
      WHEN 6 THEN 'Sábado'
    END AS nome_dia_semana,

    EXTRACT(WEEK FROM d.data) AS semana_do_ano,

    CASE 
      WHEN EXTRACT(MONTH FROM d.data) BETWEEN 1 AND 2 THEN 'Bim 1º'
      WHEN EXTRACT(MONTH FROM d.data) BETWEEN 3 AND 4 THEN 'Bim 2º'
      WHEN EXTRACT(MONTH FROM d.data) BETWEEN 5 AND 6 THEN 'Bim 3º'
      WHEN EXTRACT(MONTH FROM d.data) BETWEEN 7 AND 8 THEN 'Bim 4º'
      WHEN EXTRACT(MONTH FROM d.data) BETWEEN 9 AND 10 THEN 'Bim 5º'
      WHEN EXTRACT(MONTH FROM d.data) BETWEEN 11 AND 12 THEN 'Bim 6º'
    END AS bimestre,

    CASE 
      WHEN EXTRACT(MONTH FROM d.data) BETWEEN 1 AND 3 THEN 'Tri 1º'
      WHEN EXTRACT(MONTH FROM d.data) BETWEEN 4 AND 6 THEN 'Tri 2º'
      WHEN EXTRACT(MONTH FROM d.data) BETWEEN 7 AND 9 THEN 'Tri 3º'
      ELSE 'Tri 4º'
    END AS trimestre,

    CASE 
      WHEN EXTRACT(MONTH FROM d.data) BETWEEN 1 AND 6 THEN 'Sem 1º'
      ELSE 'Sem 2º'
    END AS semestre,

    TO_CHAR(d.data, 'YYYY-MM-DD') AS data_formatada,
    TO_CHAR(d.data, 'YYYY-MM') AS ano_mes,
    CONCAT(EXTRACT(YEAR FROM d.data), '-', LPAD(EXTRACT(MONTH FROM d.data)::TEXT, 2, '0')) AS chave_ano_mes

  FROM DATAS AS d
  LEFT JOIN  NOME_MESES AS m
    ON EXTRACT(MONTH FROM d.data) = m.numero_mes
)

SELECT * FROM CALENDARIO
ORDER BY data ASC;
