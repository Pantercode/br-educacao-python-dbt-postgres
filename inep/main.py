import os, argparse
from .orquestrador import OrquestradorExtracao

def ler_float(env: str, default: float) -> float:
    try:
        return float(os.environ.get(env, default))
    except Exception:
        return default

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrator INEP OOP (RAW/SILVER)")
    parser.add_argument("--modo", choices=["RAW","SILVER","AMBOS"], default="AMBOS")
    parser.add_argument("--endpoints", required=True)
    parser.add_argument("--schema_raw", default="raw")
    parser.add_argument("--schema_silver", default="silver")
    parser.add_argument("--lote", type=int, default=int(os.environ.get("TAMANHO_LOTE", 1000)))
    parser.add_argument("--pausa", type=float, default=ler_float("PAUSA_API", 0.3))
    args = parser.parse_args()

    base_url = os.environ.get("ENDERECO_BASE_API", "http://api.dadosabertosinep.org/v1")
    orq = OrquestradorExtracao(
        base_url=base_url,
        schema_raw=args.schema_raw,
        schema_silver=args.schema_silver,
        pausa=args.pausa,
        tamanho_lote=args.lote,
        caminho_endpoints_yaml=args.endpoints,
    )
    orq.executar()
