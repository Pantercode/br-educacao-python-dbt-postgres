import os
import time
import subprocess
from pathlib import Path
from typing import List, Dict, Tuple

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv

# ------------------------------------------------------------------ #
# Configuração
# ------------------------------------------------------------------ #
load_dotenv()

API_BASE = os.getenv("ENDERECO_BASE_API", "http://api.dadosabertosinep.org/v1")

# Tabela de redes aceitas
REDES = ["municipal", "estadual", "federal", "publica"]

# Endpoints IBGE
IBGE_API_ESTADOS = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
IBGE_API_MUNICIPIOS_UF = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"

# Paginação
TAMANHO_LOTE = int(os.getenv("TAMANHO_LOTE", "1000"))
PAUSA_API = float(os.getenv("PAUSA_API", "0.3"))

# Caminhos
BASE_DIR = Path(__file__).resolve().parents[1]
ENDPOINTS_FILE = Path(__file__).parent / "config" / "endpoints.yaml"
SEEDS_DIR = BASE_DIR / "dbt" / "seeds"

# ------------------------------------------------------------------ #
# Funções utilitárias
# ------------------------------------------------------------------ #
def obter_ufs() -> List[str]:
    """Retorna lista de UFs via API do IBGE (fallback para lista fixa)."""
    fallback = [
        "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
        "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
        "RS","RO","RR","SC","SP","SE","TO"
    ]
    try:
        resp = requests.get(IBGE_API_ESTADOS, timeout=30)
        resp.raise_for_status()
        return sorted([e["sigla"] for e in resp.json()])
    except Exception:
        return fallback

def obter_municipios_por_uf(uf: str) -> List[int]:
    """Retorna lista de códigos IBGE (inteiros) dos municípios de uma UF."""
    url = IBGE_API_MUNICIPIOS_UF.format(uf=uf)
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        return [m["id"] for m in resp.json()]
    except Exception:
        return []

def fetch_endpoint(path: str) -> pd.DataFrame:
    """Baixa dados paginados de um endpoint genérico da API INEP."""
    page = 1
    dados = []
    while True:
        url = f"{API_BASE}/{path}?page={page}&limit={TAMANHO_LOTE}"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        lote = r.json()
        if not lote:
            break
        dados.extend(lote)
        if len(lote) < TAMANHO_LOTE:
            break
        page += 1
        time.sleep(PAUSA_API)
    return pd.DataFrame(dados)

# ------------------------------------------------------------------ #
# Pipeline principal
# ------------------------------------------------------------------ #
def main():
    # ------------------------------------------------------------------ #
    # Preparação
    # ------------------------------------------------------------------ #
    with open(ENDPOINTS_FILE, "r", encoding="utf-8") as f:
        endpoints_cfg = yaml.safe_load(f)

    SEEDS_DIR.mkdir(parents=True, exist_ok=True)
    ufs = obter_ufs()

    # Cache de municípios para evitar chamadas repetidas
    cache_municipios: Dict[str, List[int]] = {}

    for ep in endpoints_cfg.get("endpoints", []):
        name: str = ep["name"]
        path_template: str = ep["path"]
        per_uf: bool = ep.get("per_uf", False)
        per_municipio: bool = ep.get("per_municipio", False)
        per_rede: bool = ep.get("per_rede", False)

        # Gera todas as combinações necessárias
        combos: List[Tuple[str, int, str]] = [(None, None, None)]  # (uf, municipio, rede)
        if per_municipio:
            combos.clear()
            for uf in ufs:
                if uf not in cache_municipios:
                    cache_municipios[uf] = obter_municipios_por_uf(uf)
                for municipio_cod in cache_municipios[uf]:
                    combos.append((uf, municipio_cod, None))
                    if per_rede:
                        combos.extend((uf, municipio_cod, rede) for rede in REDES)
        elif per_uf and per_rede:
            combos = [(uf, None, rede) for uf in ufs for rede in REDES]
        elif per_uf:
            combos = [(uf, None, None) for uf in ufs]
        elif per_rede:
            combos = [(None, None, rede) for rede in REDES]

        # Acumula resultados de todas as combinações
        frames = []

        for uf, municipio_cod, rede in combos:
            path = path_template
            if uf:
                path = path.replace("{uf}", uf)
            if municipio_cod is not None:
                path = path.replace("{municipio}", str(municipio_cod))
            if rede:
                path = path.replace("{rede}", rede)

            ident = "_".join(filter(None, [name, uf or "", str(municipio_cod) if municipio_cod else "", rede or ""]))
            print(f"→ Extraindo {ident} …")

            df = fetch_endpoint(path)
            if df.empty:
                print(f"   ⚠️ Sem dados ({ident})")
                continue

            # Anexa colunas de contexto
            if uf:
                df["uf"] = uf
            if municipio_cod is not None:
                df["municipio_cod"] = municipio_cod
            if rede:
                df["rede"] = rede

            frames.append(df)

        if not frames:
            print(f"⏭ Nenhum dado para {name}")
            continue

        final_df = pd.concat(frames, ignore_index=True).astype(str)
        csv_path = SEEDS_DIR / f"{name}.csv"
        final_df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"{name}.csv gerado — {len(final_df)} linhas.")

    # ------------------------------------------------------------------ #
    # dbt
    # ------------------------------------------------------------------ #
    print("Executando dbt seed & run …")
    subprocess.run(["dbt", "seed"], check=True)
    subprocess.run(["dbt", "run"], check=True)
    print("Pipeline concluído!")

if __name__ == "__main__":
    main()