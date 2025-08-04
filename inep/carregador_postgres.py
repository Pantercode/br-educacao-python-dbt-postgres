import os
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict, Any
from .util import log, normalizar_nome_coluna, json_dumps_compacto

class CarregadorPostgres:
    def __init__(self):
        self.host = os.environ.get("POSTGRES_HOST", "localhost")
        self.port = int(os.environ.get("POSTGRES_PORT", 5432))
        self.db   = os.environ.get("POSTGRES_DB", "postgres")
        self.user = os.environ.get("POSTGRES_USER", "postgres")
        self.pw   = os.environ.get("POSTGRES_PASSWORD", "postgres")

    def conn(self):
        return psycopg2.connect(host=self.host, port=self.port, dbname=self.db, user=self.user, password=self.pw)

    def garantir_schemas(self, schemas: List[str]) -> None:
        with self.conn() as c, c.cursor() as cur:
            for s in schemas:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {s};")
        log(f"Schemas garantidos: {schemas}")

    def criar_tabela_raw(self, schema_raw: str) -> None:
        with self.conn() as c, c.cursor() as cur:
            cur.execute(sql_raw := (
                f"CREATE TABLE IF NOT EXISTS {schema_raw}.inep_api_raw (\n"
                "    id SERIAL PRIMARY KEY,\n"
                "    endpoint TEXT NOT NULL,\n"
                "    payload JSON NOT NULL,\n"
                "    coletado_em TIMESTAMP NOT NULL DEFAULT NOW()\n"
                ");\n"
            ))
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{schema_raw}_raw_endpoint ON {schema_raw}.inep_api_raw (endpoint);")
        log("Tabela RAW pronta.")

    def gravar_raw(self, schema_raw: str, endpoint: str, registros: List[Dict[str, Any]]) -> int:
        if not registros:
            return 0
        valores = [(endpoint, json_dumps_compacto(r)) for r in registros]
        with self.conn() as c, c.cursor() as cur:
            execute_values(cur,
                f"INSERT INTO {schema_raw}.inep_api_raw (endpoint, payload) VALUES %s",
                valores
            )
        log(f"RAW: inseridos {len(registros)} registros em {schema_raw}.inep_api_raw")
        return len(registros)

    def _colunas_existentes(self, schema_silver: str, tabela: str) -> List[str]:
        with self.conn() as c, c.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (schema_silver, tabela)
            )
            return [r[0] for r in cur.fetchall()]

    def _criar_tabela_silver(self, schema_silver: str, tabela: str, colunas: List[str]) -> None:
        cols = ",\n".join([f'    "{c}" TEXT' for c in colunas])
        with self.conn() as c, c.cursor() as cur:
            cur.execute(f'CREATE TABLE IF NOT EXISTS {schema_silver}."{tabela}" (\n{cols}\n);')
        log(f"SILVER: tabela {schema_silver}.{tabela} criada/garantida.")

    def _alterar_adicionar_colunas(self, schema_silver: str, tabela: str, novas: List[str]) -> None:
        if not novas:
            return
        with self.conn() as c, c.cursor() as cur:
            for col in novas:
                cur.execute(f'ALTER TABLE {schema_silver}."{tabela}" ADD COLUMN IF NOT EXISTS "{col}" TEXT;')
        log(f"SILVER: {len(novas)} colunas novas adicionadas em {schema_silver}.{tabela}.")

    def gravar_silver(self, schema_silver: str, tabela: str, linhas: List[Dict[str, Any]]) -> int:
        if not linhas:
            return 0
        colunas = sorted({k for linha in linhas for k in linha.keys()})
        colunas = [normalizar_nome_coluna(c) for c in colunas]
        self._criar_tabela_silver(schema_silver, tabela, colunas)
        existentes = set(self._colunas_existentes(schema_silver, tabela))
        novas = [c for c in colunas if c not in existentes]
        self._alterar_adicionar_colunas(schema_silver, tabela, novas)
        finais = self._colunas_existentes(schema_silver, tabela)
        valores = []
        for linha in linhas:
            linha_norm = {normalizar_nome_coluna(k): (None if v == "" else v) for k,v in linha.items()}
            valores.append([linha_norm.get(c) for c in finais])
        cols_sql = ", ".join([f'"{c}"' for c in finais])
        with self.conn() as c, c.cursor() as cur:
            execute_values(cur,
                f'INSERT INTO {schema_silver}."{tabela}" ({cols_sql}) VALUES %s',
                valores
            )
        log(f"SILVER: inseridas {len(linhas)} linhas em {schema_silver}.{tabela}")
        return len(linhas)

    def criar_indices_basicos(self, schema_silver: str, tabela: str) -> None:
        candidatos = ["id", "id_escola", "codigo_escola", "ano", "uf", "municipio", "data"]
        with self.conn() as c, c.cursor() as cur:
            for col in candidatos:
                try:
                    cur.execute(f'CREATE INDEX IF NOT EXISTS ix_{schema_silver}_{tabela}_{col} ON {schema_silver}."{tabela}" ("{col}");')
                except Exception:
                    pass
        log(f"SILVER: índices básicos tentados para {schema_silver}.{tabela}")
