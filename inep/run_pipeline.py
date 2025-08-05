# -*- coding: utf-8 -*-
"""
CKAN MG -> tudo como TEXTO
- Usa package_show para listar resources
- Se tiver DataStore, pagina e baixa tudo
- Senão, baixa o arquivo do resource['url']
- Salva em dbt/seeds/<resource-name>.csv (tudo dtype=str)
- Roda dbt seed/run
"""
import csv
import re
import subprocess
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parents[1]
SEEDS_DIR = BASE_DIR / "dbt" / "seeds"
SEEDS_DIR.mkdir(parents=True, exist_ok=True)

CKAN_API = "https://dados.mg.gov.br/api/3/action"
# ID do dataset (UUID que aparece na sua URL)
DATASET_ID = "9150f9a1-8465-4a02-921f-e852b65e2d64"


def datastore_all_records(resource_id: str, page_size: int = 10000):
    """Itera por todas as páginas do DataStore."""
    start = 0
    total = None
    session = requests.Session()
    while True:
        params = {"resource_id": resource_id, "limit": page_size, "offset": start}
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
        for rec in records:
            yield rec
        start += len(records)
        if total is not None and start >= total:
            break


def try_download_file(url: str, encoding: Optional[str] = None) -> pd.DataFrame:
    """Baixa um arquivo e lê como CSV/JSON mantendo tudo como texto."""
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()

    ctype = resp.headers.get("Content-Type", "").lower()
    if "text/html" in ctype:
        raise RuntimeError(f"Conteúdo HTML em {url}")

    # JSON
    if "json" in ctype or url.endswith(".json"):
        df = pd.json_normalize(resp.json())
        return df.astype("string")

    # CSV com fallback de separador
    text = resp.content.decode(encoding or "utf-8", errors="replace")
    for sep in [",", ";", "\t", "|"]:
        try:
            df = pd.read_csv(
                pd.io.common.StringIO(text),
                sep=sep,
                engine="python",
                quoting=csv.QUOTE_MINIMAL,
                dtype="string",
                keep_default_na=False,   # não transforma "NA" em NaN
                on_bad_lines="skip",
            )
            return df
        except Exception:
            continue

    # sniffer por último
    try:
        first = next((ln for ln in text.splitlines() if ln.strip()), "")
        sniffer = csv.Sniffer().sniff(first)
        df = pd.read_csv(
            pd.io.common.StringIO(text),
            sep=sniffer.delimiter,
            dtype="string",
            keep_default_na=False,
            on_bad_lines="skip",
        )
        return df
    except Exception as e:
        raise RuntimeError(f"Falha ao interpretar arquivo {url}: {e}")


def safe_name(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9_-]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "recurso"


def main():
    # 1) Descobre os resources desse dataset
    print(f"CKAN package_show id={DATASET_ID}")
    pkg = requests.get(f"{CKAN_API}/package_show", params={"id": DATASET_ID}, timeout=120)
    pkg.raise_for_status()
    pj = pkg.json()
    assert pj.get("success"), f"package_show falhou: {pj}"

    resources = pj["result"].get("resources", [])
    if not resources:
        print("Nenhum resource no dataset.")
        return

    for r in resources:
        r_name = safe_name(r.get("name") or r.get("id") or "recurso")
        r_id = r.get("id")
        r_url = r.get("url") or ""
        r_fmt = (r.get("format") or "").lower()
        r_enc = r.get("encoding") or "utf-8"
        r_active = r.get("datastore_active", False)

        print(f"Processando resource: name={r_name} id={r_id} fmt={r_fmt} datastore_active={r_active} url={r_url}")

        df: Optional[pd.DataFrame] = None

        # 2) Tenta DataStore (melhor opção)
        if r_id and r_active:
            rows = list(datastore_all_records(r_id, page_size=10000))
            if rows:
                df = pd.DataFrame(rows).astype("string")

        # 3) Fallback: baixa o arquivo do resource.url
        if df is None and r_url:
            df = try_download_file(r_url, encoding=r_enc)

        if df is None:
            print(f"❌ Não foi possível obter dados do resource {r_name}")
            continue

        out = SEEDS_DIR / f"{r_name}.csv"
        df.to_csv(out, index=False, encoding="utf-8")
        print(f"✅ Salvo: {out} ({len(df)} linhas)")

    # 4) dbt
    subprocess.run(
        ["dbt", "seed", "--project-dir", "dbt", "--profiles-dir", "/opt/app/dbt/profiles", "--profile", "inep_postgres"],
        check=True,
    )
    subprocess.run(
        ["dbt", "run", "--project-dir", "dbt", "--profiles-dir", "/opt/app/dbt/profiles", "--profile", "inep_postgres"],
        check=True,
    )


if __name__ == "__main__":
    main()
