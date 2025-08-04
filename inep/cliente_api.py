import time, requests
from typing import Iterator, Dict, Any, List
from .util import log

class ClienteAPI:
    def __init__(self, base_url: str, pausa: float = 0.3, tamanho_lote: int = 1000):
        self.base_url = base_url.rstrip('/')
        self.pausa = pausa
        self.tamanho_lote = tamanho_lote

    def paginar(self, recurso: str, parametros_extra: Dict[str, Any] | None = None) -> Iterator[List[Dict[str, Any]]]:
        page = 1
        while True:
            params = {"page": page, "limit": self.tamanho_lote}
            if parametros_extra:
                params.update(parametros_extra)
            url = f"{self.base_url}/{recurso.lstrip('/')}"
            log(f"Consultando {url} page={page} limit={self.tamanho_lote}")
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            dados = resp.json()
            if not dados:
                break
            yield dados
            page += 1
            time.sleep(self.pausa)



