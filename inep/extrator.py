from typing import Dict, Any, List
from .cliente_api import ClienteAPI
from .carregador_postgres import CarregadorPostgres
from .util import log, achatar_json

class ExtratorEndpoint:
    def __init__(self, cliente: ClienteAPI, carregador: CarregadorPostgres, schema_raw: str, schema_silver: str, endpoint: str, nome_tabela: str):
        self.cliente = cliente
        self.carregador = carregador
        self.schema_raw = schema_raw
        self.schema_silver = schema_silver
        self.endpoint = endpoint
        self.nome_tabela = nome_tabela

    def executar(self, lote: int) -> None:
        total_raw = 0
        total_silver = 0
        for pagina in self.cliente.paginar(self.endpoint):
            total_raw += self.carregador.gravar_raw(self.schema_raw, self.endpoint, pagina)
            linhas: List[Dict[str, Any]] = []
            for registro in pagina:
                plano = {}
                achatar_json(prefixo="", dado=registro, acumulado=plano)
                linhas.append(plano)
            total_silver += self.carregador.gravar_silver(self.schema_silver, self.nome_tabela, linhas)
        self.carregador.criar_indices_basicos(self.schema_silver, self.nome_tabela)
        log(f"Endpoint {self.endpoint}: RAW={total_raw}, SILVER={total_silver}")
