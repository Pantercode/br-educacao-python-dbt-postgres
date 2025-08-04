#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
INEP Education API Harvester (com IBGE) -> Postgres 9.3

Camadas:
  - BRONZE: tabela única (inep_api_raw) com JSON cru e metadados
  - SILVER: tabelas achatadas por endpoint (tudo TEXT), com colunas meta

Ajustes para Postgres 9.3:
  - Usar JSON (não JSONB)
  - Sem "INSERT ... ON CONFLICT" (tratar duplicidade por exceção)
"""

import os
import io
import re
import json
import time
import hashlib
import logging
import argparse
from typing import Dict, List, Any, Optional

import requests
import pandas as pd
import psycopg2
from psycopg2 import sql

try:
    from tqdm import tqdm
except Exception:
    tqdm = lambda x, **k: x

API_BASE = "http://api.dadosabertosinep.org/v1"
UFS = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]
REDES = ["municipal","estadual","federal","publica"]
IBGE_API_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

PG_USER = os.getenv("PGUSER", "postgres")
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB   = os.getenv("PGDATABASE", "censo")
PG_PASS = os.getenv("PGPASSWORD", "123")

RAW_TABLE    = "inep_api_raw"
TBL_IDEB_ESC = "api_ideb_escolas"
TBL_IDEB_UF  = "api_ideb_resumo_uf"
TBL_IDEB_ESC_ID = "api_ideb_escola_id"

REQUEST_TIMEOUT = 60
RETRIES = 3
BACKOFF = 1.7

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("inep_crawler")

def sanitize_col(name: str) -> str:
    n = (name or "").strip()
    n = n.replace(" ", "_")
    n = re.sub(r"[^\w]", "_", n, flags=re.UNICODE)
    n = re.sub(r"_+", "_", n).strip("_")
    if not n:
        n = "col"
    if n[0].isdigit():
        n = "c_" + n
    return n.lower()

def fetch_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code in (404, 501):
                return None
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt >= RETRIES:
                raise
            time.sleep(BACKOFF ** attempt)

def hash_row(endpoint: str, params: Dict[str, Any], payload: Any) -> str:
    blob = json.dumps({"endpoint": endpoint, "params": params, "payload": payload}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

def ensure_raw_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS public.{RAW_TABLE} (
            id SERIAL PRIMARY KEY,
            endpoint TEXT NOT NULL,
            params JSON,
            payload JSON NOT NULL,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            sha256 TEXT UNIQUE
        );
        """)
        conn.commit()
        # criar índices; se já existem, ignorar
        for idx_sql in [
            f"CREATE INDEX idx_{RAW_TABLE}_endpoint ON public.{RAW_TABLE}(endpoint);",
            f"CREATE INDEX idx_{RAW_TABLE}_fetched_at ON public.{RAW_TABLE}(fetched_at);"
        ]:
            try:
                cur.execute(idx_sql)
                conn.commit()
            except Exception:
                conn.rollback()

def insert_raw(conn, endpoint: str, params: Dict[str, Any], payload: Any):
    sha = hash_row(endpoint, params or {}, payload)
    with conn.cursor() as cur:
        try:
            cur.execute(
                f"INSERT INTO public.{RAW_TABLE} (endpoint, params, payload, sha256) VALUES (%s, %s::json, %s::json, %s);",
                (endpoint, json.dumps(params or {}), json.dumps(payload), sha)
            )
            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()  # duplicado, ignore

def ensure_flat_table(conn, table: str, columns: List[str]):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema='public' AND table_name=%s
            );
        """, (table,))
        exists, = cur.fetchone()
        if not exists:
            cols_sql = ", ".join([f"{sanitize_col(c)} TEXT" for c in columns] + ["endpoint TEXT", "params JSON", "fetched_at TIMESTAMPTZ"])
            cur.execute(sql.SQL("CREATE TABLE {}.{} ({});").format(sql.Identifier("public"), sql.Identifier(table), sql.SQL(cols_sql)))
            conn.commit()
        else:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s;
            """, (table,))
            existing = {row[0] for row in cur.fetchall()}
            missing = [sanitize_col(c) for c in columns if sanitize_col(c) not in existing]
            for c in missing:
                try:
                    cur.execute(sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} TEXT;")
                                .format(sql.Identifier("public"), sql.Identifier(table), sql.Identifier(c)))
                    conn.commit()
                except Exception:
                    conn.rollback()
            for meta_col, ddl in [("endpoint","TEXT"), ("params","JSON"), ("fetched_at","TIMESTAMPTZ")]:
                if meta_col not in existing:
                    try:
                        cur.execute(sql.SQL(f"ALTER TABLE public.{table} ADD COLUMN {meta_col} {ddl};"))
                        conn.commit()
                    except Exception:
                        conn.rollback()

def df_to_postgres(conn, table: str, df: pd.DataFrame):
    if df.empty:
        return
    for c in ["endpoint","params","fetched_at"]:
        if c not in df.columns:
            df[c] = None
    ensure_flat_table(conn, table, [c for c in df.columns if c not in {"endpoint","params","fetched_at"}])
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    col_idents = [sql.Identifier(c) for c in df.columns]
    copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '\\N')").format(
        sql.Identifier("public"), sql.Identifier(table), sql.SQL(", ").join(col_idents)
    )
    with conn.cursor() as cur:
        cur.copy_expert(copy_sql.as_string(conn), buf)
    conn.commit()

def json_list_to_df(payload: Any) -> pd.DataFrame:
    if payload is None:
        return pd.DataFrame()
    if isinstance(payload, list):
        if not payload:
            return pd.DataFrame()
        return pd.json_normalize(payload, max_level=1)
    elif isinstance(payload, dict):
        return pd.json_normalize([payload], max_level=1)
    else:
        return pd.DataFrame()

def add_meta(df: pd.DataFrame, endpoint: str, params: Dict[str, Any]) -> pd.DataFrame:
    df = df.copy()
    df.columns = [sanitize_col(c) for c in df.columns]
    df["endpoint"] = endpoint
    df["params"] = json.dumps(params or {}, ensure_ascii=False)
    df["fetched_at"] = pd.Timestamp.utcnow()
    for c in df.columns:
        if df[c].apply(lambda x: isinstance(x, (list, dict))).any():
            df[c] = df[c].apply(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v)
    return df

def get_municipios() -> pd.DataFrame:
    data = fetch_json(IBGE_API_MUNICIPIOS)
    if not data:
        raise RuntimeError("Falha ao carregar municípios do IBGE.")
    rows = []
    for m in data:
        micro = m.get("microrregiao") or {}
        meso = (micro.get("mesorregiao") or {})
        uf = (meso.get("UF") or {})
        rows.append({
            "id_municipio_ibge": m.get("id"),
            "nome_municipio": m.get("nome"),
            "sigla_uf": uf.get("sigla"),
        })
    return pd.DataFrame(rows)

def crawl_ideb_escolas(conn, mode: str, sleep: float):
    endpoint = "/ideb/escolas.json"
    combos = [{"uf": uf} for uf in UFS]
    if mode.upper() == "FULL":
        combos += [{"uf": uf, "rede": rede} for uf in UFS for rede in REDES]
        muns = get_municipios()
        combos += [{"codigo_municipio": int(row.id_municipio_ibge)} for _, row in muns.iterrows()]

    for params in tqdm(combos, desc="ideb/escolas"):
        url = f"{API_BASE}{endpoint}"
        try:
            payload = fetch_json(url, params=params)
            if payload is None:
                continue
            insert_raw(conn, endpoint, params, payload)
            df = json_list_to_df(payload)
            if not df.empty:
                df = add_meta(df, endpoint, params)
                df_to_postgres(conn, TBL_IDEB_ESC, df)
            if sleep:
                time.sleep(sleep)
        except Exception as e:
            log.error("Erro %s params=%s: %s", endpoint, params, e)

def crawl_ideb_resumo_uf(conn, sleep: float):
    endpoint = "/ideb.json"
    for uf in tqdm(UFS, desc="ideb resumo UF"):
        params = {"uf": uf}
        url = f"{API_BASE}{endpoint}"
        try:
            payload = fetch_json(url, params=params)
            if payload is None:
                continue
            insert_raw(conn, endpoint, params, payload)
            df = json_list_to_df(payload)
            if not df.empty:
                df = add_meta(df, endpoint, params)
                df_to_postgres(conn, TBL_IDEB_UF, df)
            if sleep:
                time.sleep(sleep)
        except Exception as e:
            log.error("Erro %s UF=%s: %s", endpoint, uf, e)

def crawl_ideb_escola_id(conn, sleep: float):
    endpoint = "/ideb/escola/{codigo}.json"
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s AND column_name ~* '(codigo|cod).*escola';
        """, (TBL_IDEB_ESC,))
        code_cols = [r[0] for r in cur.fetchall()]

    if not code_cols:
        log.warning("Não achei coluna de código da escola em %s. Pulei %s.", TBL_IDEB_ESC, endpoint)
        return

    code_col = code_cols[0]
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT DISTINCT {} FROM public.{} WHERE {} IS NOT NULL LIMIT 1000000;")
                    .format(sql.Identifier(code_col), sql.Identifier(TBL_IDEB_ESC), sql.Identifier(code_col)))
        codes = [r[0] for r in cur.fetchall()]

    for code in tqdm(codes, desc="ideb/escola/{codigo}"):
        if code is None or str(code).strip() == "":
            continue
        params = {}
        url = f"{API_BASE}/ideb/escola/{code}.json"
        try:
            payload = fetch_json(url, params=params)
            if payload is None:
                continue
            insert_raw(conn, "/ideb/escola/{codigo}.json", {"codigo": code}, payload)
            df = json_list_to_df(payload)
            if not df.empty:
                df = add_meta(df, "/ideb/escola/{codigo}.json", {"codigo": code})
                df_to_postgres(conn, TBL_IDEB_ESC_ID, df)
            if sleep:
                time.sleep(sleep)
        except Exception as e:
            log.error("Erro /ideb/escola/%s.json: %s", code, e)

def main():
    parser = argparse.ArgumentParser(description="Coleta da API do INEP (IDEB) -> Postgres 9.3")
    parser.add_argument("--mode", choices=["SAFE", "FULL"], default="SAFE", help="SAFE: por UF; FULL: UF + rede + município")
    parser.add_argument("--sleep", type=float, default=0.2, help="Segundos entre requisições")
    args = parser.parse_args()

    # Remove interação por getpass — senha já definida automaticamente acima
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    
    with conn.cursor() as cur:
        cur.execute("SET client_encoding TO 'UTF8';")
    conn.commit()

    ensure_raw_table(conn)

    t0 = time.time()
    crawl_ideb_escolas(conn, args.mode, args.sleep)
    crawl_ideb_resumo_uf(conn, args.sleep)
    crawl_ideb_escola_id(conn, args.sleep)
    t1 = time.time()

    log.info("Finalizado em %.1fs | modo=%s | DB=%s@%s", (t1 - t0), args.mode, PG_DB, PG_HOST)
    conn.close()

if __name__ == "__main__":
    main()

