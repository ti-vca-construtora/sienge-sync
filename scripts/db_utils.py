"""
Utilitários compartilhados de banco de dados.
Substitui SQL Server (mssql+pyodbc) por PostgreSQL (psycopg2).
"""
import os
import math
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    host = os.getenv('PG_HOST', 'localhost')
    port = os.getenv('PG_PORT', '5432')
    database = os.getenv('PG_DATABASE')
    username = os.getenv('PG_USERNAME')
    password = os.getenv('PG_PASSWORD')
    connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    return create_engine(connection_string)


def chunked_iterable(iterable, size):
    return (iterable[i:i + size] for i in range(0, len(iterable), size))


def delete_in_batches_by_billid(bill_ids, table_name, connection, batch_size=900):
    """Deleta registros em lotes por billId."""
    total_deleted = 0
    bill_ids = [str(x) for x in bill_ids]
    for batch in chunked_iterable(bill_ids, batch_size):
        placeholders = ', '.join(f':id_{i}' for i in range(len(batch)))
        params = {f'id_{i}': v for i, v in enumerate(batch)}
        result = connection.execute(
            text(f"DELETE FROM {table_name} WHERE \"billId\" IN ({placeholders})"),
            params
        )
        total_deleted += result.rowcount or 0
        print(f"  Deletados até agora: {total_deleted} registros...")
    print(f"Deleção completada. Total: {total_deleted}")
    return total_deleted


def delete_in_batches_by_verificador(verificadores, table_name, connection, batch_size=900):
    """Deleta registros em lotes por verificador."""
    total_deleted = 0
    for batch in chunked_iterable(list(verificadores), batch_size):
        placeholders = ', '.join(f':v_{i}' for i in range(len(batch)))
        params = {f'v_{i}': v for i, v in enumerate(batch)}
        result = connection.execute(
            text(f"DELETE FROM {table_name} WHERE verificador IN ({placeholders})"),
            params
        )
        total_deleted += result.rowcount or 0
        print(f"  Deletados até agora: {total_deleted} registros...")
    print(f"Deleção completada. Total: {total_deleted}")
    return total_deleted


def insert_in_batches(df, table_name, connection, batch_size=500):
    """Insere DataFrame em lotes no PostgreSQL."""
    total_rows = len(df)
    inserted_rows = 0
    total_chunks = math.ceil(total_rows / batch_size)
    for idx, start_idx in enumerate(range(0, total_rows, batch_size), start=1):
        batch_df = df.iloc[start_idx:start_idx + batch_size]
        batch_df.to_sql(table_name, con=connection, if_exists='append', index=False)
        inserted_rows += len(batch_df)
        print(f"  Chunk {idx}/{total_chunks} — {inserted_rows}/{total_rows} inseridos ({inserted_rows/total_rows*100:.1f}%)")
    print(f"Inserção completada. Total: {total_rows}")


def log_to_file(log_filename_prefix, success, total_deleted=0, total_rows=0, error_message=None):
    timestamp = datetime.now().strftime('%d-%m-%Y-%H-%M')
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, f"{log_filename_prefix}_{timestamp}.txt")
    if success:
        content = f"Atualização bem-sucedida\n\n{total_deleted} registros excluídos\n{total_rows} registros inseridos\n"
    else:
        content = f"Erro na atualização\n\nErro:\n{error_message}\n"
    with open(log_filepath, "w") as f:
        f.write(content)
    print(f"Log gravado em: {log_filepath}")
