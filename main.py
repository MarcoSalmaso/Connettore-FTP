from ftplib import FTP
import io
import time
import functions_framework
import psycopg2
import os

FTP_DB_MAP = {
    "anagrafiche": [
      {"cartella_ftp": "BRANDS_EXPORT", "tabella": "anagrafiche.brands"},
      {"cartella_ftp": "COLLECTIONS_EXPORT", "tabella": "anagrafiche.collections"},
      {"cartella_ftp": "LOCATIONS_EXPORT", "tabella": "anagrafiche.locations"},
      {"cartella_ftp": "PAYMENT_METHODS_EXPORT", "tabella": "anagrafiche.payment_methods"},
      {"cartella_ftp": "SUBJECTS_EXPORT", "tabella": "anagrafiche.subjects"},
      {"cartella_ftp": "SUBJECTS_RELATIONS_EXPORT", "tabella": "anagrafiche.subjects_relations"},
      {"cartella_ftp": "SUPPLIERS_EXPORT", "tabella": "anagrafiche.suppliers"},
      {"cartella_ftp": "PRICELIST_EXPORT", "tabella": "anagrafiche.pricelist"},
      {"cartella_ftp": "PRICELIST_SHOPS_EXPORT", "tabella": "anagrafiche.pricelist_shops"},
      {"cartella_ftp": "BRAND_SUPPLIER_EXPORT", "tabella": "anagrafiche.brand_supplier"}
    ],
    "clienti": [
      {"cartella_ftp": "CLIENTS_CARD_EXPORT", "tabella": "clienti.clients_cards"},
      {"cartella_ftp": "CLIENTS_DESTINATION_EXPORT", "tabella": "clienti.clients_destinations"},
      {"cartella_ftp": "CLIENTS_EXPORT", "tabella": "clienti.clients"},
      {"cartella_ftp": "CLIENTS_EXTERNAL_MAPPINGS", "tabella": "clienti.clients_external_mappings"},
      {"cartella_ftp": "CLIENTS_PRIVACY_EXPORT", "tabella": "clienti.clients_privacy"},
      {"cartella_ftp": "CLIENTS_REFERENCES_EXPORT", "tabella": "clienti.clients_references"}
    ],
    "prodotti": [
      {"cartella_ftp": "PRODUCTS_EXPORT", "tabella": "prodotti.products"},
      {"cartella_ftp": "PRODUCTS_ATTRIBUTES_EXPORT", "tabella": "prodotti.products_attributes"},
      {"cartella_ftp": "PRODUCTS_BARCODES_EXPORT", "tabella": "prodotti.products_barcodes"},
      {"cartella_ftp": "PRODUCTS_CLASSIFICATIONS_EXPORT", "tabella": "prodotti.products_classifications"},
      {"cartella_ftp": "PRODUCTS_SUPPLIERS_EXPORT", "tabella": "prodotti.products_suppliers"},
      {"cartella_ftp": "PRODUCTS_VARIANTS_EXPORT", "tabella": "prodotti.products_variants"},
      {"cartella_ftp": "PRODUCTS_VARIANTS_GROUP_EXPORT", "tabella": "prodotti.products_variants_group"}
    ],
    "magazzino": [
      {"cartella_ftp": "STOCK_EXPORT", "tabella": "magazzino.stock"}
    ],
    "vendite": [
      {"cartella_ftp": "SALE_PROFILE_EXPORT", "tabella": "vendite.sale_profiles"},
      {"cartella_ftp": "SALE_PROFILE_SHOPS_EXPORT", "tabella": "vendite.sale_profile_shops"}
    ]
}

def get_connection():
    """
    Crea una connessione al database PostgreSQL utilizzando le credenziali fornite.
    """
    config = {
        "host": os.getenv("host"),
        "port": os.getenv("port"),
        "dbname": os.getenv("dbname"),
        "user": os.getenv("user"),
        "password": os.getenv("password")
    }

    try:
        conn = psycopg2.connect(**config)
        # print("Connessione effettuata!")
        return conn
    except Exception as e:
        print(f"-> ❌ Errore durante la connessione al database '{config['dbname']}': {e}")
        return None


def close_connection(conn, cursor=None):
    """
    Chiude il cursore e la connessione al database se esistono.
    """
    try:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        # print("Connessione chiusa!")
    except Exception as e:
        print(f"-> ❌ Errore durante la chiusura della connessione: {e}")


def ensure_schema(schema_name):
    conn = get_connection()
    if not conn:
        print(f"-> ❌ Connessione al DB fallita per la creazione dello schema {schema_name}.")
        return
    try:
        cursor = conn.cursor()
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
        conn.commit()
        print(f"✅ Schema '{schema_name}' verificato o creato.")
    except Exception as e:
        print(f"-> ❌ Errore durante la creazione dello schema {schema_name}: {e}")
    finally:
        close_connection(conn, cursor)


def execute_query(query, params=None):
    """
    Esegue una query di selezione.
    """
    conn = get_connection()
    records = []
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            records = cursor.fetchall()
        except Exception as e:
            print(f"-> ❌ Errore durante l'esecuzione della query: {e}")
        finally:
            close_connection(conn, cursor)
    return records


def download_file_from_ftp(directory):
    host = os.getenv("ftp_host")
    username = os.getenv("ftp_username")
    password = os.getenv("ftp_password")
    remote_dir = f"/{directory}"

    ftp = FTP(host)
    ftp.login(user=username, passwd=password)
    ftp.cwd(remote_dir)

    files = ftp.nlst()
    latest_file = sorted(files)[-1]

    print(f"📥 Scarico il file {latest_file}...")

    file_buffer = io.BytesIO()
    ftp.retrbinary(f'RETR {latest_file}', file_buffer.write)

    ftp.quit()

    file_buffer.seek(0)
    return file_buffer, latest_file


def create_table_if_not_exists(table_name, columns_types):
    # table_name can be schema.table or just table
    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        schema, table = None, table_name

    conn = get_connection()
    if not conn:
        print(f"-> ❌ Connessione al DB fallita per la creazione della tabella {table_name}.")
        return
    try:
        cursor = conn.cursor()
        if schema:
            ensure_schema(schema)
            fqtn = f'"{schema}"."{table}"'
        else:
            fqtn = f'"{table}"'
        cols = ", ".join([f'"{col}" {typ}' for col, typ in columns_types])
        query = f'CREATE TABLE IF NOT EXISTS {fqtn} ({cols});'
        cursor.execute(query)
        conn.commit()
        print(f"✅ Tabella {fqtn} verificata o creata.")
    except Exception as e:
        print(f"-> ❌ Errore durante la creazione della tabella {table_name}: {e}")
    finally:
        close_connection(conn, cursor)


def import_csv_to_db(file_buffer, table_name, delete_existing=False):
    file_buffer.seek(0)

    # Parse schema.table
    if "." in table_name:
        schema, table = table_name.split(".", 1)
        fqtn = f'"{schema}"."{table}"'
    else:
        schema, table = None, table_name
        fqtn = f'"{table}"'

    conn = get_connection()
    if not conn:
        print(f"-> ❌ Connessione al DB fallita per la tabella {table_name}.")
        return
    try:
        cursor = conn.cursor()

        text_buffer = io.TextIOWrapper(file_buffer, encoding="utf-8")
        header_line = next(text_buffer)
        columns = [col.strip() for col in header_line.strip().split(",")]

        create_table_if_not_exists(table_name, [(col, "TEXT") for col in columns])

        if delete_existing:
            print(f"🗑️  TRUNCATE dei dati esistenti in {fqtn}...")
            cursor.execute(f'TRUNCATE TABLE {fqtn};')

        copy_sql = f'''
            COPY {fqtn} ({', '.join([f'"{c}"' for c in columns])})
            FROM STDIN WITH (FORMAT csv, NULL '', DELIMITER ',', HEADER FALSE)
        '''
        cursor.copy_expert(sql=copy_sql, file=text_buffer)
        conn.commit()
        print(f"✅ Dati caricati in {fqtn}.")
    except Exception as e:
        print(f"-> ❌ Errore durante l'import in {table_name} (COPY): {e}")
    finally:
        close_connection(conn, cursor)


@functions_framework.http
def main(request):

    global_start = time.time()

    # Flatten mapping entries (we import only those listed in FTP_DB_MAP)
    entries = []
    for categoria, items in FTP_DB_MAP.items():
        for it in items:
            entries.append((it["cartella_ftp"], it["tabella"]))

    # Import: all mapped tables are treated as full refresh (svuotiamo e ricarichiamo)
    for ftp_dir, fq_table in entries:
        start_time = time.time()
        print(f"🔄 Caricamento completo: FTP '{ftp_dir}' → DB '{fq_table}'")
        try:
            buffer, latest = download_file_from_ftp(ftp_dir)
            print(f"📥 File scaricato: {latest}")
            import_csv_to_db(buffer, fq_table, delete_existing=True)
        except Exception as e:
            print(f"-> ❌ Errore su {ftp_dir} → {fq_table}: {e}")
        end_time = time.time()
        print(f"⏱️  Tempo di caricamento per {fq_table}: {end_time - start_time:.2f} secondi.\n")
    
    # Connessione al database per eseguire la funzione
    conn = get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            print("📦 Esecuzione delle function postgresql...")
            cursor.execute("SELECT aggiorna_stock_export_info();")
            cursor.execute("SELECT esegui_riassortimento();")
            conn.commit()
            print("✅ Funzioni eseguite con successo!")
        except Exception as e:
            print(f"-> ❌ Errore durante l'esecuzione della funzione: {e}")
        finally:
            close_connection(conn, cursor)

    # Tempo totale di esecuzione
    global_end = time.time()
    elapsed_time = global_end - global_start
    print(f"⏱️  Tempo totale di esecuzione: {elapsed_time:.2f} secondi.")

    return '✅ Caricamento completato per tutte le tabelle!'