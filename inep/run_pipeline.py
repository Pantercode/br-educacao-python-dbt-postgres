# -------------------- inep/run_pipeline.py (usar DataStore CKAN primeiro) --------------------
# - Lê o datapackage do portal MG
# - Para cada resource com 'id', usa CKAN Data API (datastore_search) e pagina todos os registros
# - Se o resource não estiver no DataStore, tenta baixar o arquivo pelo 'path' com fallback de separadores

import subprocess
from pathlib import Path
from urllib.parse import urljoin, quote
import math
import csv
import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parents[1]
SEEDS_DIR = BASE_DIR / "dbt" / "seeds"
SEEDS_DIR.mkdir(parents=True, exist_ok=True)

CKAN_API = "https://dados.mg.gov.br/api/3/action"
DATAPACKAGE_URL = "https://dados.mg.gov.br/dataset/9150f9a1-8465-4a02-921f-e852b65e2d64/resource/920d0e17-73f4-4788-bdb8-7d09b6a4ccdd/download/datapackage.json"


def datastore_all_records(resource_id: str, page_size: int = 10000):
    """Itera por todas as páginas do DataStore e rende registros."""
    start = 0
    total = None
    session = requests.Session()
    while True:
        params = {
            "resource_id": resource_id,
            "limit": page_size,
            "offset": start,
        }
        r = session.get(f"{CKAN_API}/datastore_search", params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            raise RuntimeError(f"datastore_search falhou: {data}")
        res = data["result"]
        if total is None:
            total = res.get("total")
        records = res.get("records", [])
        if not records:
            break
        yield from records
        start += len(records)
        if total is not None and start >= total:
            break


def try_download_file(full_url: str, encoding: str | None = None) -> pd.DataFrame:
    """Baixa um arquivo e tenta ler como CSV com sniff/fallback; se JSON, normaliza."""
    resp = requests.get(full_url, timeout=300)
    resp.raise_for_status()

    # Se vier HTML, aborta (é página de erro, não dado)
    ctype = resp.headers.get("Content-Type", "").lower()
    if "text/html" in ctype:
        raise RuntimeError(f"Conteúdo HTML em {full_url}")

    content = resp.content
    # Tenta JSON primeiro se o content-type sugerir
    if "json" in ctype or full_url.endswith(".json"):
        return pd.json_normalize(resp.json())

    # Tenta CSV com autodetecção de separador
    text = content.decode(encoding or "utf-8", errors="replace")
    for sep in [",", ";", "\t", "|"]:
        try:
            return pd.read_csv(pd.io.common.StringIO(text), sep=sep, engine="python", quoting=csv.QUOTE_MINIMAL)
        except Exception:
            continue
    # Sniffer como último recurso
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(text.splitlines()[0])
        return pd.read_csv(pd.io.common.StringIO(text), sep=dialect.delimiter)
    except Exception as e:
        raise RuntimeError(f"Falha ao interpretar arquivo {full_url}: {e}")


def candidate_file_urls(dataset_id: str, resource_id: str, path: str) -> list[str]:
    """Gera URLs candidatas para baixar arquivo quando DataStore não está ativo."""
    filename = path.split("/")[-1]
    base_download = f"https://dados.mg.gov.br/dataset/{dataset_id}/resource/{resource_id}/download/"
    return [
        urljoin(base_download, path),       # .../download/data/bolsas.csv
        urljoin(base_download, filename),   # .../download/bolsas.csv
    ]


def main():
    print(f"Lendo datapackage: {DATAPACKAGE_URL}")
    dp = requests.get(DATAPACKAGE_URL, timeout=120)
    dp.raise_for_status()
    pkg = dp.json()
    resources = pkg.get("resources", [])
    if not resources:
        print("Nenhum resource no datapackage.")
        return

    # dataset/resource IDs a partir da URL do datapackage
    # (útil para montar URLs de download)
    import re
    m = re.search(r"/dataset/([^/]+)/resource/([^/]+)/download/", DATAPACKAGE_URL)
    dataset_id = m.group(1) if m else ""
    resource_id_from_url = m.group(2) if m else ""

    for r in resources:
        name = (r.get("name") or "recurso").strip().replace(" ", "_")
        rid = r.get("id")  # GUID do CKAN DataStore
        path = r.get("path") or r.get("url") or ""
        fmt = (r.get("format") or "").lower()
        encoding = r.get("encoding") or "utf-8"

        print(f"Processando resource: {name} (id={rid}, fmt={fmt}, path={path})")
        df = None

        # 1) Tenta DataStore se houver 'id'
        if rid:
            # checa se o DataStore está ativo para este resource
            try:
                rs = requests.get(f"{CKAN_API}/resource_show", params={"id": rid}, timeout=60)
                rs.raise_for_status()
                meta = rs.json()
                active = meta.get("result", {}).get("datastore_active", False)
            except Exception:
                active = False

            if active:
                print(f"→ Baixando via DataStore (paginado) id={rid}")
                # stream em pedaços para evitar memória alta
                rows = []
                for rec in datastore_all_records(rid, page_size=10000):
                    rows.append(rec)
                    # flush em lotes grandes
                    if len(rows) >= 200000:
                        part = pd.DataFrame(rows)
                        rows.clear()
                        out = SEEDS_DIR / f"{name}.csv"
                        # append incremental
                        header = not out.exists()
                        part.to_csv(out, index=False, encoding="utf-8", mode="a", header=header)
                        print(f"  ↳ flush parcial: {len(part)} linhas → {out}")
                # resto
                if rows:
                    part = pd.DataFrame(rows)
                    out = SEEDS_DIR / f"{name}.csv"
                    header = not out.exists()
                    part.to_csv(out, index=False, encoding="utf-8", mode="a", header=header)
                    print(f"  ↳ flush final: {len(part)} linhas → {out}")
                df = None  # já gravado em partes
            else:
                print("⚠️ DataStore inativo; tentando download direto do arquivo…")

        # 2) Fallback: tentar baixar arquivo pelo path/url
        if df is None and path:
            # tenta URLs candidatas com base no dataset/resource da própria URL do datapackage
            for url in candidate_file_urls(dataset_id or "", rid or resource_id_from_url or "", path):
                try:
                    print(f"Tentando {url}")
                    df = try_download_file(url, encoding=encoding)
                    break
                except Exception as e:
                    print(f"   falhou: {e}")
                    df = None

        if df is None:
            print(f"❌ Não foi possível obter dados de {name}")
            continue

        out = SEEDS_DIR / f"{name}.csv"
        df.to_csv(out, index=False, encoding="utf-8")
        print(f"✅ Salvo: {out} ({len(df)} linhas)")

    # roda dbt (usa profile 'inep_postgres')
    subprocess.run(["dbt", "seed", "--project-dir", "dbt", "--profiles-dir", "/opt/app/dbt/profiles", "--profile", "inep_postgres"], check=True)
    subprocess.run(["dbt", "run",  "--project-dir", "dbt", "--profiles-dir", "/opt/app/dbt/profiles", "--profile", "inep_postgres"], check=True)

if __name__ == "__main__":
    main()
