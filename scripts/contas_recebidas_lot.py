"""
Atualiza TB_CONTASRECEBIDAS_LOT — empresa LOT.
Mesma lógica do VCA, mas usa credenciais LOT e endpoint /vcalotear/.
"""
import requests
import base64
import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Dict, List, Any
from db_utils import get_engine, delete_in_batches_by_verificador, insert_in_batches, log_to_file

load_dotenv()

# ----------------------------
# Configuração
# ----------------------------
TABLE_NAME = 'TB_CONTASRECEBIDAS_LOT'
LOG_PREFIX = 'log_contasrecebidas_lot'

api_key_lot = os.getenv('API_KEY_LOT')
source_lot = os.getenv('API_USER_LOT')

init_date = '2000-01-01'
end_date = '2999-12-31'
type_date = 'D'
correction_date = '2020-01-01'
delaydays = 7
changeStartDate = (datetime.today() - timedelta(days=delaydays)).strftime('%Y-%m-%d')

encoded_credentials = base64.b64encode(f"{source_lot}:{api_key_lot}".encode()).decode()
headers = {
    "Content-type": "application/json",
    "X-origin-request": "WorksheetModel",
    "Authorization": f"Basic {encoded_credentials}"
}

# ----------------------------
# Normalização JSON (idêntica ao VCA)
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
        receipts = row.get('receipts', []) or []

        if not receipts:
            continue

        for receipt_category in receipts_categories:
            receipt_category_data = {f'receiptsCategories.{k}': v for k, v in receipt_category.items()}
            try:
                rate = float(receipt_category.get('financialCategoryRate') or 0) / 100.0
            except Exception:
                rate = 0

            for receipt in receipts:
                receipt_data = {
                    'receipts.operationTypeId': receipt.get('operationTypeId'),
                    'receipts.operationTypeName': receipt.get('operationTypeName'),
                    'receipts.grossAmount': receipt.get('grossAmount'),
                    'receipts.netAmount': receipt.get('netAmount'),
                    'receipts.paymentDate': receipt.get('paymentDate'),
                    'receipts.monetaryCorrectionAmount': receipt.get('monetaryCorrectionAmount'),
                    'receipts.interestAmount': receipt.get('interestAmount'),
                    'receipts.fineAmount': receipt.get('fineAmount'),
                    'receipts.discountAmount': receipt.get('discountAmount'),
                    'receipts.taxAmount': receipt.get('taxAmount'),
                    'receipts.additionAmount': receipt.get('additionAmount'),
                    'receipts.insuranceAmount': receipt.get('insuranceAmount'),
                    'receipts.dueAdmAmount': receipt.get('dueAdmAmount'),
                    'receipts.calculationDate': receipt.get('calculationDate'),
                    'receipts.accountCompanyId': receipt.get('accountCompanyId'),
                    'receipts.accountNumber': receipt.get('accountNumber'),
                    'receipts.accountType': receipt.get('accountType'),
                    'receipts.sequencialNumber': receipt.get('sequencialNumber'),
                    'receipts.correctedNetAmount': receipt.get('correctedNetAmount'),
                    'receipts.indexerId': receipt.get('indexerId'),
                    'receipts.embeddedInterestAmount': receipt.get('embeddedInterestAmount'),
                    'receipts.proRata': receipt.get('proRata'),
                }

                combined_record = {**base_record, **receipt_category_data, **receipt_data}

                amount_keys = [
                    'originalAmount', 'balanceAmount', 'correctedBalanceAmount',
                    'discountAmount', 'taxAmount', 'embeddedInterestAmount',
                    'receipts.monetaryCorrectionAmount', 'receipts.interestAmount',
                    'receipts.fineAmount', 'receipts.discountAmount', 'receipts.taxAmount',
                    'receipts.additionAmount', 'receipts.insuranceAmount', 'receipts.dueAdmAmount',
                    'receipts.correctedNetAmount', 'receipts.embeddedInterestAmount',
                    'receipts.proRata', 'receipts.grossAmount', 'receipts.netAmount'
                ]
                for key in amount_keys:
                    source_dict = receipt_data if 'receipts.' in key else base_record
                    val = source_dict.get(key)
                    combined_record[f'{key}_part'] = val * rate if val is not None else 0

                combined_record['verificador'] = (
                    f"{combined_record.get('billId','')}_{combined_record.get('documentNumber','')}"
                    f"_{combined_record.get('installmentNumber','')}_{combined_record.get('paymentTerm.id','')}"
                    f"_{combined_record.get('installmentId','')}_{combined_record.get('receipts.paymentDate','')}"
                ).replace(' ', '')

                expanded_records.append(combined_record)

    final_df = pd.DataFrame(expanded_records)
    if final_df.empty:
        return final_df

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
        'receipts.monetaryCorrectionAmount_part': 'monetaryCorrectionAmount',
        'receipts.interestAmount_part': 'interestAmount',
        'receipts.fineAmount_part': 'fineAmount',
        'receipts.discountAmount_part': 'receiptsDiscountAmount',
        'receipts.taxAmount_part': 'receiptsTaxAmount',
        'receipts.additionAmount_part': 'additionAmount',
        'receipts.insuranceAmount_part': 'insuranceAmount',
        'receipts.dueAdmAmount_part': 'dueAdmAmount',
        'receipts.correctedNetAmount_part': 'correctedNetAmount',
        'receipts.embeddedInterestAmount_part': 'receiptsEmbeddedInterestAmount',
        'receipts.proRata_part': 'proRata',
        'receipts.grossAmount_part': 'grossAmount',
        'receipts.netAmount_part': 'netAmount',
        'receipts.operationTypeId': 'operationTypeId',
        'receipts.operationTypeName': 'operationTypeName',
        'receipts.paymentDate': 'paymentDate',
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
        'correctionType', 'interestBaseDate', 'defaulterSituation', 'subJudicie',
        'operationTypeId', 'operationTypeName', 'grossAmount', 'netAmount',
        'monetaryCorrectionAmount', 'interestAmount', 'fineAmount',
        'receiptsDiscountAmount', 'receiptsTaxAmount', 'additionAmount', 'insuranceAmount',
        'dueAdmAmount', 'correctedNetAmount', 'receiptsEmbeddedInterestAmount',
        'proRata', 'paymentDate', 'verificador'
    ]
    return final_df[[c for c in desired_columns if c in final_df.columns]]

# ----------------------------
# Coleta de dados
# ----------------------------
def fetch_data():
    print(f"[{TABLE_NAME}] Buscando dados alterados desde: {changeStartDate}")
    url = (
        f'https://api.sienge.com.br/vcalotear/public/api/bulk-data/v1/income'
        f'?startDate={init_date}&endDate={end_date}&selectionType={type_date}'
        f'&correctionDate={correction_date}&changeStartDate={changeStartDate}'
    )
    resp = requests.get(url, headers=headers, timeout=300)
    if resp.status_code != 200:
        raise Exception(f"Erro na API: {resp.status_code} / {resp.text}")

    data = resp.json().get('data', [])
    print(f"[{TABLE_NAME}] Registros retornados pela API: {len(data)}")
    return data

# ----------------------------
# Fluxo principal
# ----------------------------
def main():
    print(f"\n{'='*50}")
    print(f"Iniciando: {TABLE_NAME}")
    print(f"{'='*50}")

    try:
        data = fetch_data()
        if not data:
            print("Nenhum dado retornado. Nada a atualizar.")
            return

        df = safe_normalize_json(data)
        if df.empty:
            print("Nenhuma linha após normalização. Abortando.")
            return

        print(f"Total de linhas normalizadas: {len(df)}")

        engine = get_engine()
        with engine.begin() as conn:
            verificadores = df['verificador'].unique().tolist()
            total_deleted = delete_in_batches_by_verificador(verificadores, TABLE_NAME, conn)
            insert_in_batches(df, TABLE_NAME, conn)

        log_to_file(LOG_PREFIX, success=True, total_deleted=total_deleted, total_rows=len(df))
        print(f"[{TABLE_NAME}] Concluído com sucesso.")

    except Exception as e:
        print(f"[{TABLE_NAME}] ERRO: {e}")
        log_to_file(LOG_PREFIX, success=False, error_message=str(e))
        raise


if __name__ == "__main__":
    main()
