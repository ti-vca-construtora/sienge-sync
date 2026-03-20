"""
Atualiza TB_CONTASARECEBER — empresa VCA.
Lógica original preservada; banco migrado para PostgreSQL.
"""
import requests
import base64
import math
import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Dict, List, Any
from db_utils import get_engine, delete_in_batches_by_billid, insert_in_batches, log_to_file

load_dotenv()

# ----------------------------
# Configuração
# ----------------------------
EXPORT_XLSX = False  # desligado na VPS — sem sentido gerar Excel em servidor
TABLE_NAME = 'TB_CONTASARECEBER'
LOG_PREFIX = 'log_contasareceber'

api_key = os.getenv('API_KEY')
source = os.getenv('API_USER')

init_date = '2000-01-01'
end_date = '2999-12-31'
type_date = 'D'
correction_date = datetime.today().strftime('%Y-%m-%d')
delaydays = 7
changeStartDate = (datetime.today() - timedelta(days=delaydays)).strftime('%Y-%m-%d')

encoded_credentials = base64.b64encode(f"{source}:{api_key}".encode()).decode()
headers = {
    "Content-type": "application/json",
    "X-origin-request": "WorksheetModel",
    "Authorization": f"Basic {encoded_credentials}"
}

# ----------------------------
# Normalização JSON
# ----------------------------
def safe_normalize_json(data: List[Dict[Any, Any]]) -> pd.DataFrame:
    base_columns = [
        'companyId', 'companyName', 'clientId', 'clientName',
        'billId', 'documentIdentificationId', 'documentNumber',
        'dueDate', 'issueDate', 'billDate', 'installmentBaseDate',
        'mainUnit', 'installmentNumber', 'paymentTerm.id',
        'originalAmount', 'balanceAmount', 'correctedBalanceAmount', 'installmentId',
        'businessAreaId', 'businessAreaName', 'projectId', 'projectName',
        'groupCompanyId', 'groupCompanyName', 'holdingId', 'holdingName',
        'subsidiaryId', 'subsidiaryName', 'businessTypeId', 'businessTypeName',
        'documentForecast', 'originId', 'discountAmount', 'taxAmount', 'embeddedInterestAmount',
        'indexerId', 'indexerName', 'periodicityType', 'interestType', 'interestRate',
        'correctionType', 'interestBaseDate', 'defaulterSituation', 'subJudicie'
    ]

    df_base = pd.json_normalize(data)
    expanded_records = []

    for _, row in df_base.iterrows():
        base_record = {col: row.get(col) for col in base_columns}
        receipts_categories = row.get('receiptsCategories', []) or [{}]

        for receipt_category in receipts_categories:
            receipt_category_data = {f'receiptsCategories.{k}': v for k, v in receipt_category.items()}
            combined_record = {**base_record, **receipt_category_data}

            try:
                rate = float(receipt_category.get('financialCategoryRate') or 0) / 100.0
            except Exception:
                rate = 0

            for field in ['originalAmount', 'balanceAmount', 'correctedBalanceAmount',
                          'discountAmount', 'taxAmount', 'embeddedInterestAmount']:
                val = base_record.get(field)
                combined_record[f'{field}_part'] = val * rate if val is not None else None

            try:
                combined_record['verificador'] = (
                    f"{combined_record.get('billId','')}_{combined_record.get('documentNumber','')}"
                    f"_{combined_record.get('installmentNumber','')}_{combined_record.get('paymentTerm.id','')}"
                    f"_{combined_record.get('installmentId','')}".replace(' ', '')
                )
            except Exception:
                combined_record['verificador'] = None

            expanded_records.append(combined_record)

    final_df = pd.DataFrame(expanded_records)

    cols_to_drop = [c for c in ['originalAmount', 'balanceAmount', 'correctedBalanceAmount',
                                 'discountAmount', 'taxAmount', 'embeddedInterestAmount'] if c in final_df.columns]
    final_df.drop(columns=cols_to_drop, inplace=True)

    final_df.rename(columns={
        'paymentTerm.id': 'paymentTermId',
        'receiptsCategories.costCenterId': 'costCenterId',
        'receiptsCategories.costCenterName': 'costCenterName',
        'receiptsCategories.financialCategoryId': 'financialCategoryId',
        'receiptsCategories.financialCategoryName': 'financialCategoryName',
        'receiptsCategories.financialCategoryRate': 'financialCategoryRate',
        'receiptsCategories.financialCategoryReducer': 'financialCategoryReducer',
        'receiptsCategories.financialCategoryType': 'financialCategoryType',
        'originalAmount_part': 'originalAmount',
        'balanceAmount_part': 'balanceAmount',
        'correctedBalanceAmount_part': 'correctedBalanceAmount',
        'discountAmount_part': 'discountAmount',
        'taxAmount_part': 'taxAmount',
        'embeddedInterestAmount_part': 'embeddedInterestAmount',
    }, inplace=True)

    desired_columns = [
        'companyId', 'companyName', 'clientId', 'clientName',
        'billId', 'documentIdentificationId', 'documentNumber',
        'dueDate', 'issueDate', 'billDate', 'installmentId', 'installmentBaseDate',
        'mainUnit', 'installmentNumber', 'paymentTermId', 'originalAmount',
        'balanceAmount', 'correctedBalanceAmount', 'costCenterId', 'costCenterName',
        'financialCategoryId', 'financialCategoryName', 'financialCategoryRate',
        'financialCategoryReducer', 'financialCategoryType',
        'businessAreaId', 'businessAreaName', 'projectId', 'projectName',
        'groupCompanyId', 'groupCompanyName', 'holdingId', 'holdingName',
        'subsidiaryId', 'subsidiaryName', 'businessTypeId', 'businessTypeName',
        'documentForecast', 'originId', 'discountAmount', 'taxAmount', 'embeddedInterestAmount',
        'indexerId', 'indexerName', 'periodicityType', 'interestType', 'interestRate',
        'correctionType', 'interestBaseDate', 'defaulterSituation', 'subJudicie', 'verificador'
    ]
    return final_df[[c for c in desired_columns if c in final_df.columns]]

# ----------------------------
# Coleta de dados
# ----------------------------
def fetch_changed_bill_ids():
    print(f"[{TABLE_NAME}] Buscando billIds alterados desde: {changeStartDate}")
    url = (
        f'https://api.sienge.com.br/vca/public/api/bulk-data/v1/income'
        f'?startDate={init_date}&endDate={end_date}&selectionType={type_date}'
        f'&correctionDate={correction_date}&changeStartDate={changeStartDate}'
    )
    resp = requests.get(url, headers=headers, timeout=300)
    if resp.status_code != 200:
        raise Exception(f"Erro ao buscar billIds: {resp.status_code} / {resp.text}")

    data = resp.json().get('data', [])
    bill_ids = list({item['billId'] for item in data if item.get('billId') is not None})
    print(f"[{TABLE_NAME}] Total de billIds encontrados: {len(bill_ids)}")
    return bill_ids


def fetch_full_bills(bill_ids, chunk_size=1000):
    if not bill_ids:
        return pd.DataFrame()

    all_raw = []
    total_chunks = math.ceil(len(bill_ids) / chunk_size)
    print(f"[{TABLE_NAME}] Buscando dados completos em {total_chunks} chunks...")

    for idx, i in enumerate(range(0, len(bill_ids), chunk_size), start=1):
        chunk = bill_ids[i:i + chunk_size]
        ids_str = ','.join(str(x) for x in chunk)
        url = (
            f'https://api.sienge.com.br/vca/public/api/bulk-data/v1/income/by-bills'
            f'?billsIds={ids_str}&correctionDate={correction_date}'
        )
        print(f"  Chunk {idx}/{total_chunks} => {len(chunk)} billIds")
        resp = requests.get(url, headers=headers, timeout=300)
        if resp.status_code != 200:
            raise Exception(f"Erro no by-bills (status {resp.status_code}): {resp.text}")
        data = resp.json().get('data', [])
        if data:
            all_raw.extend(data)

    df = safe_normalize_json(all_raw)
    print(f"[{TABLE_NAME}] Total de linhas após normalização: {len(df)}")
    return df

# ----------------------------
# Fluxo principal
# ----------------------------
def main():
    print(f"\n{'='*50}")
    print(f"Iniciando: {TABLE_NAME}")
    print(f"{'='*50}")

    try:
        bill_ids = fetch_changed_bill_ids()
        if not bill_ids:
            print("Nenhum billId alterado. Nada a atualizar.")
            return

        df = fetch_full_bills(bill_ids)
        if df.empty:
            print("Nenhum dado retornado. Abortando.")
            return

        df = df[(df['correctedBalanceAmount'].notna()) & (df['correctedBalanceAmount'] != 0)]
        print(f"Linhas após filtro correctedBalanceAmount: {len(df)}")

        engine = get_engine()
        with engine.begin() as conn:
            total_deleted = delete_in_batches_by_billid(bill_ids, TABLE_NAME, conn)
            insert_in_batches(df, TABLE_NAME, conn)

        log_to_file(LOG_PREFIX, success=True, total_deleted=total_deleted, total_rows=len(df))
        print(f"[{TABLE_NAME}] Concluído com sucesso.")

    except Exception as e:
        print(f"[{TABLE_NAME}] ERRO: {e}")
        log_to_file(LOG_PREFIX, success=False, error_message=str(e))
        raise


if __name__ == "__main__":
    main()
