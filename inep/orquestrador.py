import yaml
from .cliente_api import ClienteAPI
from .carregador_postgres import CarregadorPostgres
from .extrator import ExtratorEndpoint
from .util import log

class OrquestradorExtracao:
    def __init__(self, base_url: str, schema_raw: str, schema_silver: str, pausa: float, tamanho_lote: int, caminho_endpoints_yaml: str):
        self.base_url = base_url
        self.schema_raw = schema_raw
        self.schema_silver = schema_silver
        self.pausa = pausa
        self.tamanho_lote = tamanho_lote
        self.caminho_endpoints_yaml = caminho_endpoints_yaml

    def carregar_endpoints(self):
        with open(self.caminho_endpoints_yaml, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data.get("endpoints", [])

    def executar(self):
        loader = CarregadorPostgres()
        loader.garantir_schemas([self.schema_raw, self.schema_silver, "gold"])
        loader.criar_tabela_raw(self.schema_raw)
        cliente = ClienteAPI(base_url=self.base_url, pausa=self.pausa, tamanho_lote=self.tamanho_lote)
        for ep in self.carregar_endpoints():
            recurso = ep["recurso"]
            tabela  = ep.get("tabela_silver") or recurso.replace("/", "_").replace(".json", "")
            extrator = ExtratorEndpoint(cliente, loader, self.schema_raw, self.schema_silver, recurso, tabela)
            log(f"Iniciando endpoint: {recurso} -> tabela silver: {tabela}")
            extrator.executar(self.tamanho_lote)
