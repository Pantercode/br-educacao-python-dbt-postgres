
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extrai os dados do IBGE de estados e municípios e grava como CSV local.
"""

import requests
import pandas as pd

API_ESTADOS = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
API_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

def baixar_estados():
    r = requests.get(API_ESTADOS)
    r.raise_for_status()
    data = r.json()
    df = pd.json_normalize(data)
    df.to_csv("estados_ibge.csv", index=False, encoding="utf-8-sig")
    print("Arquivo 'estados_ibge.csv' criado com sucesso.")

def baixar_municipios():
    r = requests.get(API_MUNICIPIOS)
    r.raise_for_status()
    data = r.json()
    registros = []
    for m in data:
        micro = m.get("microrregiao") or {}
        meso = micro.get("mesorregiao") or {}
        uf = meso.get("UF") or {}
        registros.append({
            "id_municipio_ibge": m.get("id"),
            "nome_municipio": m.get("nome"),
            "sigla_uf": uf.get("sigla"),
            "nome_uf": uf.get("nome"),
            "regiao": uf.get("regiao", {}).get("nome")
        })
    df = pd.DataFrame(registros)
    df.to_csv("municipios_ibge.csv", index=False, encoding="utf-8-sig")
    print("Arquivo 'municipios_ibge.csv' criado com sucesso.")

if __name__ == "__main__":
    baixar_estados()
    baixar_municipios()
