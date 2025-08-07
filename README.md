# Projeto **br\_educacao** – Pipeline ETL com Python + dbt em Docker 🐳

Extrai dados públicos do INEP/IBGE, gera **seeds** para o dbt e materializa camadas **Bronze → Silver → Gold** em PostgreSQL, prontas para BI e análises.

---

## ✨ Visão geral

| Camada             | Conteúdo                           | Materialização          | Schema de destino |
| ------------------ | ---------------------------------- | ----------------------- | ----------------- |
| **Seeds (Bronze)** | CSVs brutos baixados da API        | `dbt seed` → **tabela** | `bronze`          |
| **Raw**            | *Views* que reorganizam colunas    | `view`                  | `bronze`          |
| **Silver**         | Colunas tipadas, sem joins         | `table`                 | `silver`          |
| **Gold**           | Métricas, joins e lógica analítica | `table`                 | `gold`            |

---

## 🏗️ Arquitetura

```text
API INEP/IBGE ─▶ Python extractor (pipeline) ─▶ dbt seed ─▶ PostgreSQL
                                           └▶ dbt run  ─▶ Bronze/Silver/Gold
```

* **pipeline**: contêiner Python que instala dependências, baixa CSVs e executa `dbt seed && dbt run`
* **dbt-cli**: imagem oficial para comandos interativos (`dbt run`, `dbt test`, etc.)
* **postgres**: banco PostgreSQL 9.3, exposto em `localhost:5432`

---

## 📂 Estrutura de diretórios

```
.
├─ dbt_project.yml
├─ .env
├─ docker-compose.yml
├─ dbt/
│   ├─ models/
│   │   ├─ raw/
│   │   ├─ silver/
│   │   └─ gold/
│   └─ profiles/          # profiles.yml do dbt
└─ seeds/                 # CSVs gerados pelo extractor
```

---

## ⚙️ Configuração rápida

Crie um arquivo `.env` com:

```dotenv
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=censo
POSTGRES_USER=postgres
POSTGRES_PASSWORD=123
```

Dependências principais definidas no `requirements.txt`:

```
dbt-core==1.7.14
dbt-postgres==1.7.14
pandas==2.2.2
requests==2.32.3
PyYAML==6.0.2
```

---

## 🚀 Como rodar

### 1. Pipeline completo (extract + seed + run)

```bash
docker compose up --build pipeline       # Primeira execução
# ou
docker compose run --rm pipeline         # Execuções subsequentes
```

### 2. Desenvolvimento por partes

```bash
# Sobe apenas o Postgres
docker compose up -d postgres

# Recarrega os CSVs (seeds)
docker compose run --rm dbt-cli dbt seed

# Executa os modelos da camada Silver
docker compose run --rm dbt-cli dbt run --select silver

# Executa os modelos da camada Gold
docker compose run --rm dbt-cli dbt run --select gold

# Executa testes (se houverem)
docker compose run --rm dbt-cli dbt test
```

Sugestão de alias para facilitar:

```bash
alias dbt="docker compose run --rm dbt-cli"
```

---

## 🗄️ Configuração dbt (`dbt_project.yml`)

```yaml
name: br_educacao
version: "1.0.0"
config-version: 2

profile: inep_postgres

model-paths: ["models"]
seed-paths:  ["seeds"]

seeds:
  br_educacao:
    +schema: bronze            # Raw via CSV

models:
  br_educacao:
    raw:                       # models/raw/*
      +schema: bronze
      +materialized: view

    silver:                    # models/silver/*
      +schema: silver
      +materialized: table

    gold:                      # models/gold/*
      +schema: gold
      +materialized: table
```

---

## ✍️ Contribuindo

1. Faça um *fork* e crie uma branch com uma feature ou correção.
2. Mantenha commits pequenos e bem descritos.
3. Rode `dbt test` antes de abrir um PR.
4. Explique claramente a motivação no pull request.

---

## 📜 Licença

MIT — use, modifique e distribua à vontade.
Veja o arquivo `LICENSE` para mais informações.

---

## 📌 TL;DR

1. **Clone** o repositório
2. Ajuste o `.env` com as variáveis do Postgres
3. Execute `docker compose up --build pipeline`
4. Conecte-se ao schema **gold** e consuma no Power BI

✅ Pipeline reprodutível
✅ Dados versionados
✅ Preparado para orquestração futura
✅ 100% Docker
