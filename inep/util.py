import re, json
from typing import Dict, Any

def log(msg: str) -> None:
    print(f"[INEP] {msg}")

def json_dumps_compacto(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def achatar_json(prefixo: str, dado: Any, acumulado: Dict[str, str]) -> None:
    if isinstance(dado, dict):
        for k, v in dado.items():
            chave = f"{prefixo}_{k}" if prefixo else str(k)
            achatar_json(chave, v, acumulado)
    elif isinstance(dado, list):
        acumulado[prefixo] = json_dumps_compacto(dado)
    else:
        acumulado[prefixo] = "" if dado is None else str(dado)

def normalizar_nome_coluna(nome: str) -> str:
    nome = re.sub(r"[^0-9A-Za-z_]+", "_", nome)
    nome = re.sub(r"(__+)", "_", nome).strip("_").lower()
    if nome in {"user","order","group","schema","table"}:
        nome = f"col_{nome}"
    return nome[:60]
