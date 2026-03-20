"""
Orquestrador principal — roda os 4 scripts em sequência.
É esse arquivo que o cron chama. Se um script falhar, os demais continuam.
"""
import sys
import traceback
from datetime import datetime

from contas_receber import main as receber_vca
from contas_receber_lot import main as receber_lot
from contas_recebidas import main as recebidas_vca
from contas_recebidas_lot import main as recebidas_lot

JOBS = [
    ("TB_CONTASARECEBER",       receber_vca),
    ("TB_CONTASARECEBER_LOT",   receber_lot),
    ("TB_CONTASRECEBIDAS",      recebidas_vca),
    ("TB_CONTASRECEBIDAS_LOT",  recebidas_lot),
]

def run():
    start = datetime.now()
    print(f"\n{'#'*60}")
    print(f"# INÍCIO DO JOB — {start.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'#'*60}\n")

    results = {}
    for name, job_fn in JOBS:
        try:
            job_fn()
            results[name] = "OK"
        except Exception as e:
            results[name] = f"ERRO: {e}"
            traceback.print_exc()

    end = datetime.now()
    elapsed = end - start

    print(f"\n{'#'*60}")
    print(f"# RESUMO — duração: {elapsed}")
    print(f"{'#'*60}")
    for name, status in results.items():
        icon = "✓" if status == "OK" else "✗"
        print(f"  {icon}  {name}: {status}")
    print()

    # Sai com código de erro se qualquer job falhou (útil para cron alertar)
    if any(s != "OK" for s in results.values()):
        sys.exit(1)

if __name__ == "__main__":
    run()
