import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, ValueProvider
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, Dict, Literal
from datetime import datetime
import re
import datetime as dt
import pymysql

# --- Configuración de logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Opciones personalizadas para el pipeline ---
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input_db_uri',
            type=str,
            help='URI de la base de datos de entrada (mysql+pymysql://user:password@host:port/db)'
        )
        parser.add_value_provider_argument(
            '--output_db_uri',
            type=str,
            help='URI de la base de datos de salida'
        )
        parser.add_value_provider_argument(
            '--log_table',
            type=str,
            default='etl_log',
            help='Nombre de la tabla de logs'
        )
        parser.add_value_provider_argument(
            '--process_all',
            type=str,
            default='False',
            help='Flag para forzar procesamiento completo (True/False)'
        )

# --- Funciones compartidas ---
def create_mysql_engine(db_uri: str):
    # Add pool_recycle to prevent connections from becoming stale.
    # Set to a value less than MySQL's wait_timeout (e.g., 3600 seconds = 1 hour).
    # Consider connect_args={'connect_timeout': 10} for connection timeout if needed.
    return create_engine(db_uri, pool_recycle=3600)

def stream_query_results(query: str, db_uri: str, params: Optional[tuple] = None):
    """
    Executes a SQL query and streams the results row by row.
    Each row is yielded as a dictionary.
    """
    engine = create_mysql_engine(db_uri)
    with engine.connect() as conn:
        result_proxy = conn.execute(text(query), params)
        for row_mapping in result_proxy.mappings(): # .mappings() yields dict-like RowMapping objects
            yield dict(row_mapping) # Convert to standard dict

def read_table(db_uri: str, table_name: str, log_table: Optional[str], process_all: bool):
    current_params: Optional[tuple] = None
    if process_all or not log_table:
        query = f"SELECT * FROM {table_name}"
        # No params needed for this query
    else:
        query = f"""
            SELECT * FROM {table_name}
            WHERE fecha_actualizacion > (
                SELECT COALESCE(MAX(ultima_fecha), '1900-01-01')
                FROM {log_table}
                WHERE tabla_origen = %s  -- pymysql uses %s for positional placeholders
            )
        """
        current_params = (table_name,)

    for record_dict in stream_query_results(query, db_uri, params=current_params):
        # Ensure conversion to standard Python types, especially datetime
        # record_dict is already a dict from stream_query_results
        yield {k: (v.to_pydatetime() if isinstance(v, pd.Timestamp) else v) for k, v in record_dict.items()}

def clean_string(value):
    if value is None:
        return None
    return re.sub(r'\s+', ' ', str(value).strip())

def convert_date(value):
    if not value:
        return None
    if isinstance(value, (dt.datetime, dt.date)):
        # Si es un objeto date, convertir a datetime (a medianoche) para consistencia
        if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
            return dt.datetime.combine(value, dt.time.min)
        return value
    if isinstance(value, str):
        try:
            # Intentar parsear formato ISO 8601 primero
            return dt.datetime.fromisoformat(value)
        except ValueError:
            try:
                # Fallback a formato YYYY-MM-DD
                return dt.datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                logger.warning(f"No se pudo parsear la cadena de fecha: {value} con formatos ISO o %Y-%m-%d.")
                return None
    logger.warning(f"Tipo no soportado para conversión de fecha: {type(value)}, valor: {value}")
    return None

def normalize_desc(desc):
    if desc is None:
        return ""
    return re.sub(r'\s+', ' ', desc.strip().lower())

def format_datetime_to_iso(dt_obj: Optional[dt.datetime]) -> Optional[str]:
    if dt_obj is None:
        return None
    return dt_obj.isoformat()

# --- Funciones para control de logs ---
class GetTablesDoFn(beam.DoFn):
    def __init__(self, input_db_uri, log_table, process_all_flag):
        self.input_db_uri = input_db_uri
        self.log_table = log_table
        self.process_all_flag = process_all_flag

    def process(self, element):
        input_db_uri_value = self.input_db_uri.get()
        log_table_value = self.log_table.get()
        process_all_value = self.process_all_flag.get().lower() == 'true'
        
        engine = create_engine(input_db_uri_value)
        tables_to_process = []

        try:
            with engine.connect() as conn:
                if process_all_value:
                    tables_to_process = [
                        'SalesFINAL12312016', 'PurchasesFINAL12312016',
                        'BegInvFINAL12312016', 'EndInvFINAL12312016',
                        'PurchasePricesDec2017', 'InvoicePurchases12312016'
                    ]
                else:
                    query_logs = text(f"SELECT tabla_origen, ultima_fecha FROM {log_table_value}")
                    log_results = conn.execute(query_logs).mappings().all()

                    for row in log_results:
                        tabla = row['tabla_origen']
                        ultima_fecha = row['ultima_fecha']

                        query_max_fecha = text(f"SELECT MAX(fecha_actualizacion) as max_fecha FROM {tabla}")
                        max_fecha_result = conn.execute(query_max_fecha).mappings().fetchone()
                        max_fecha = max_fecha_result['max_fecha'] if max_fecha_result else None

                        if max_fecha and max_fecha > ultima_fecha:
                            tables_to_process.append(tabla)

        except Exception as e:
            logger.error(f"Error al consultar tabla de logs: {e}")
            raise

        yield tables_to_process

class UpdateLogDoFn(beam.DoFn):
    def __init__(self, input_db_uri, log_table):
        self.input_db_uri = input_db_uri
        self.log_table = log_table
        self.engine = None
        self.log_table_value = None

    def setup(self):
        # Resolver ValueProviders y crear el engine una vez por instancia de DoFn
        self.engine = create_engine(self.input_db_uri.get())
        self.log_table_value = self.log_table.get()

    def process(self, element):
        table_to_log = element # element es el nombre de la tabla (string)
        now = datetime.now()

     #Asegurarse de que el engine y log_table_value estén inicializados
        if not self.engine or not self.log_table_value:
            logger.warning("UpdateLogDoFn: Engine o log_table_value no inicializados en setup. Intentando inicializar ahora.")
            self.setup() # Reintentar setup o manejar el error apropiadamente
            if not self.engine or not self.log_table_value:
                logger.error("UpdateLogDoFn: Falló la inicialización del engine o log_table_value.")
                raise RuntimeError("UpdateLogDoFn: No se pudo inicializar el engine o log_table_value.")
            
        try:
            with self.engine.begin() as conn:
                # 'element' ya es el nombre de la tabla individual que se debe procesar.
                query = text(f"""
                    INSERT INTO {self.log_table_value} (tabla_origen, ultima_fecha)
                    VALUES (:table_name, :now)
                    ON DUPLICATE KEY UPDATE ultima_fecha = :now
                """)
                conn.execute(query, {'table_name': table_to_log, 'now': now}) # Usar table_to_log
            logger.info(f"Tabla de logs actualizada para tabla: {table_to_log}")
        except Exception as e:
            logger.error(f"Error al actualizar tabla de logs para {table_to_log}: {e}")
            raise
        
        yield element

    def teardown(self):
        if self.engine:
            self.engine.dispose()

# ==================== ETL PARA SALES ====================
class SalesETL:
    @staticmethod
    def execute(pipeline, options):
        input_db_uri = options.input_db_uri
        output_db_uri = options.output_db_uri
        log_table = options.log_table
        process_all_flag = options.process_all

        class ReadSalesData(beam.DoFn):
            def __init__(self, db_uri, table_name, log_table, process_all):
                self.db_uri = db_uri
                self.table_name = table_name
                self.log_table = log_table
                self.process_all_vp = process_all

            def process(self, element):
                db_uri_value = self.db_uri.get()
                log_table_value = self.log_table.get()
                process_all_bool = self.process_all_vp.get().lower() == 'true'
                # read_table already yields records, just yield from it
                for record in read_table(db_uri_value, self.table_name, log_table_value, process_all_bool):
                    yield record


        class TransformSalesData(beam.DoFn):
            def process(self, record):
                vendor = record.get('VendorName')
                if vendor:
                    cleaned_vendor = re.match(r'^([^\r\n]*)', str(vendor))
                    record['VendorName'] = cleaned_vendor.group(1).strip('"') if cleaned_vendor else vendor

                if not record.get('Store') or record.get('Store') == 0:
                    return
                if not record.get('SalesDollars') or record.get('SalesDollars') == 0:
                    return

                if 'Description' in record and record['Description']:
                    record['Description'] = str(record['Description']).strip()
                if 'VendorName' in record and record['VendorName']:
                    record['VendorName'] = str(record['VendorName']).strip()

                record.pop('VendorName', None)
                yield record

        def select_and_reorder_fields(record):
            return {
                'SalesQuantity': record.get('SalesQuantity'),
                'Store': record.get('Store'),
                'SalesDollars': record.get('SalesDollars'),
                'SalesPrice': record.get('SalesPrice'),
                'SalesDate': record.get('SalesDate'),
                'Volume': record.get('Volume'),
                'ExciseTax': record.get('ExciseTax'),
                'fecha_actualizacion': record.get('fecha_actualizacion'),
                'Description': record.get('Description'),
            }

        def set_fecha_actualizacion(record):
            record['fecha_actualizacion'] = datetime.now()
            return record

        class WriteSalesData(beam.DoFn):
            def __init__(self, db_uri, table_name):
                self.db_uri_vp = db_uri
                self.table_name = table_name
                self.buffer = []
                self.engine = None
                self.connection = None
                self.resolved_db_uri = None

            def setup(self):
                self.resolved_db_uri = self.db_uri_vp.get()
                self.engine = create_mysql_engine(self.resolved_db_uri)

            def start_bundle(self):
                try:
                    self.connection = self.engine.connect()
                except Exception as e:
                    logger.error(f"Failed to get DB connection in start_bundle for {self.table_name}: {e}")
                    raise

            def process(self, record):
                self.buffer.append(record)
                if len(self.buffer) >= 1000:
                    self._write_buffer()

            def finish_bundle(self):
                if self.buffer:
                    self._write_buffer()

            def _write_buffer(self):
                if not self.buffer:
                    return
                try:
                    df = pd.DataFrame(self.buffer)
                    df.to_sql(name=self.table_name, con=self.connection, if_exists='append', index=False, chunksize=500, method='multi')
                    logger.info(f"Wrote {len(self.buffer)} records to {self.table_name}")
                    self.buffer = []
                except Exception as e:
                    logger.error(f"Error writing batch to {self.table_name}: {e}")
                    raise

            def teardown(self):
                if self.connection:
                    self.connection.close()
                    self.connection = None
                if self.engine:
                    self.engine.dispose()
                    self.engine = None
                self.buffer = []

        return (
            pipeline
            | "StartSales_SalesETL" >> beam.Create([None])
            | "ReadSalesData_SalesETL" >> beam.ParDo(ReadSalesData(input_db_uri, 'SalesFINAL12312016', log_table, process_all_flag))
            | "TransformSalesData_SalesETL" >> beam.ParDo(TransformSalesData())
            | "SelectFieldsSales_SalesETL" >> beam.Map(select_and_reorder_fields)
            # Removed steps that generated SalesID
            | "SetUpdateDateSales_SalesETL" >> beam.Map(set_fecha_actualizacion)
            | "WriteSalesData_SalesETL" >> beam.ParDo(WriteSalesData(output_db_uri, 'sales'))
        )

# ==================== ETL PARA PURCHASE PRICES Y BEG INV ====================
class PurchasePriceBegInvETL:
    @staticmethod
    def execute(pipeline, options):
        input_db_uri = options.input_db_uri
        output_db_uri = options.output_db_uri
        process_all_flag = options.process_all

        class ReadPurchasePrices(beam.DoFn):
            def __init__(self, db_uri, process_all):
                self.db_uri = db_uri
                self.process_all = process_all

            def process(self, element):
                db_uri = self.db_uri.get()
                process_all = self.process_all.get().lower() == 'true'
                for record in read_table(db_uri, 'PurchasePricesDec2017', None, process_all):
                    # Ensure the yielded record is a dictionary before processing
                    if not isinstance(record, dict):
                         logger.warning(f"ReadPurchasePrices received non-dict record of type: {type(record)}, value: {str(record)[:200]}. Skipping.")
                         continue # Skip non-dictionary records

                    vendor = record.get('VendorName') # Get vendor name here, before the conditional checks

                    for k, v in record.items():
                        if isinstance(v, str) and v.strip() == '':
                            record[k] = None
                    # La siguiente lógica condicional y de limpieza debe estar correctamente indentada
                    # y no interrumpida por definiciones de funciones o clases.
                    if any(val is None for val in record.values()):
                        pass # The vendor check is now outside this block
                    if vendor: # This check is now safe as vendor is always defined
                        m = re.match(r'^([^\r\n]*)', vendor)
                        record['VendorName'] = m.group(1).strip('"').strip() if m else vendor.strip('"').strip()
                    if record.get('Volume') == 0 or record.get('Price') == 0:
                        continue
                    record['Description'] = clean_string(record.get('Description'))
                    yield record

        class ReadBegInv(beam.DoFn):
            def __init__(self, db_uri, process_all):
                self.db_uri = db_uri
                self.process_all = process_all
            
            def process(self, element):
                db_uri = self.db_uri.get()
                process_all = self.process_all.get().lower() == 'true'
                for record in read_table(db_uri, 'BegInvFINAL12312016', None, process_all):
                    # Punto de inspección:
                    logger.info(f"ReadBegInv (in PurchasePriceBegInvETL) yielding record of type: {type(record)}, value: {str(record)[:200]}")
                    # Ensure the yielded record is a dictionary before passing it on
                    if isinstance(record, dict):
                        yield record
                    else:
                        logger.warning(f"ReadBegInv (in PurchasePriceBegInvETL) received non-dict record of type: {type(record)}, value: {str(record)[:200]}. Skipping.")

        def join_purchaseprices_beginv(purchase, beginv_dict):
             # Asegurar que 'purchase' sea un diccionario
            if not isinstance(purchase, dict):
                logger.warning(f"join_purchaseprices_beginv: 'purchase' no es un diccionario. Tipo: {type(purchase)}. Saltando elemento.")
                return [] # FlatMap espera un iterable; lista vacía para descartar

            desc = purchase.get('Description') # Ahora es seguro llamar a .get()
            beginv_item = beginv_dict.get(desc)

            if beginv_item:
                if isinstance(beginv_item, dict):
                    # Ambos son diccionarios, seguro para fusionar
                    combined = {**beginv_item, **purchase}
                    return [combined] # FlatMap espera un iterable
                else:
                    # El elemento de beginv_dict no es un diccionario, registrar y devolver 'purchase' original
                    logger.warning(f"join_purchaseprices_beginv: El elemento de 'beginv_dict' para la descripción '{desc}' no es un diccionario. Tipo: {type(beginv_item)}. Devolviendo 'purchase' original.")
                    return [purchase] # Devolver 'purchase' original (que es un diccionario)
            else:
                # No se encontró join, devolver 'purchase' original
                return [purchase] # Devolver 'purchase' original (que es un diccionario)

        class WriteProductToMySQL(beam.DoFn):
            def __init__(self, db_uri, table_name):
                self.db_uri_vp = db_uri
                self.table_name = table_name
                self.buffer = []
                self.engine = None
                self.connection = None
                self.resolved_db_uri = None

            def setup(self):
                self.resolved_db_uri = self.db_uri_vp.get()
                self.engine = create_mysql_engine(self.resolved_db_uri)

            def start_bundle(self):
                try:
                    self.connection = self.engine.connect()
                except Exception as e:
                    logger.error(f"Failed to get DB connection in start_bundle for {self.table_name}: {e}")
                    raise

            def process(self, record):
                self.buffer.append(record)
                if len(self.buffer) >= 1000:
                    self._write_buffer()

            def finish_bundle(self):
                if self.buffer:
                    self._write_buffer()

            def _write_buffer(self):
                if not self.buffer:
                    return
                try:
                    df = pd.DataFrame(self.buffer)
                    df.to_sql(name=self.table_name, con=self.connection, if_exists='append', index=False, chunksize=500, method='multi')
                    logger.info(f"Wrote {len(self.buffer)} records to {self.table_name}")
                    self.buffer = []
                except Exception as e:
                    logger.error(f"Error writing batch to {self.table_name}: {e}")
                    raise

            def teardown(self):
                if self.connection:
                    self.connection.close()
                    self.connection = None
                if self.engine:
                    self.engine.dispose()
                    self.engine = None
                self.buffer = []

        purchaseprices = (
            pipeline
            | "StartPurchasePrices" >> beam.Create([None])
            | "ReadPurchasePrices" >> beam.ParDo(ReadPurchasePrices(input_db_uri, process_all_flag))
        )

        beginv_dict = (
            pipeline
            | "StartBegInv_etlpurchase" >> beam.Create([None])
            | "ReadBegInv_1" >> beam.ParDo(ReadBegInv(input_db_uri, process_all_flag))
            | "MapBegInvToKV_PPBETL" >> beam.Map(lambda x: (x.get('Description'), x)) # Mapear explícitamente a (K,V)
            | "ToDictBegInv" >> beam.combiners.ToDict() # ToDict ahora espera (K,V)
        )

        
        product_data_pcoll = (
            purchaseprices
            | "JoinWithBegInv" >> beam.FlatMap(
                join_purchaseprices_beginv,
                beginv_dict=beam.pvalue.AsSingleton(beginv_dict))
            # Removed steps that generated ProductID
            )
        _ = product_data_pcoll | "WriteProducts" >> beam.ParDo(WriteProductToMySQL(output_db_uri, "product"))
        
        return product_data_pcoll


# ==================== ETL PARA PURCHASES ====================
class PurchasesETL:
    @staticmethod
    def execute(pipeline, options):
        input_db_uri = options.input_db_uri
        output_db_uri = options.output_db_uri
        process_all_flag = options.process_all

        class ReadPurchases(beam.DoFn):
            def __init__(self, db_uri, process_all):
                self.db_uri = db_uri
                self.process_all = process_all
            
            def process(self, element):
                db_uri = self.db_uri.get()
                process_all = self.process_all.get().lower() == 'true'
                for record in read_table(db_uri, 'PurchasesFINAL12312016', None, process_all):
                    if not record.get('PurchasePrice') or record.get('PurchasePrice') == 0:
                        continue
                    record['Description'] = clean_string(record.get('Description'))
                    record['VendorName'] = clean_string(record.get('VendorName'))
                     # Convertir todos los objetos datetime y date a cadenas ISO
                    for key, value in list(record.items()): # Usar list para permitir modificación
                        if isinstance(value, dt.datetime):
                            record[key] = format_datetime_to_iso(value)
                        elif isinstance(value, dt.date): # Manejar también objetos date
                            datetime_value = dt.datetime.combine(value, dt.time.min)
                            record[key] = format_datetime_to_iso(datetime_value)
                    if not record.get('Size'):
                        record['Size'] = '750mL'
                    yield record

        class ReadInvoices(beam.DoFn):
            def __init__(self, db_uri, process_all):
                self.db_uri = db_uri
                self.process_all = process_all
            
            def process(self, element):
                db_uri = self.db_uri.get()
                process_all = self.process_all.get().lower() == 'true'
                for record in read_table(db_uri, 'InvoicePurchases12312016', None, process_all):
                    record['VendorName'] = clean_string(record.get('VendorName'))
                    # Convertir todos los objetos datetime y date a cadenas ISO
                    for key, value in list(record.items()): # Usar list para permitir modificación
                        if isinstance(value, dt.datetime):
                            record[key] = format_datetime_to_iso(value)
                        elif isinstance(value, dt.date): # Manejar también objetos date
                            datetime_value = dt.datetime.combine(value, dt.time.min)
                            record[key] = format_datetime_to_iso(datetime_value)
                    yield record

        class WritePurchasesToMySQL(beam.DoFn):
            def __init__(self, db_uri, table_name):
                self.db_uri_vp = db_uri
                self.table_name = table_name
                self.buffer = []
                self.engine = None
                self.connection = None
                self.resolved_db_uri = None

            def setup(self):
                self.resolved_db_uri = self.db_uri_vp.get()
                self.engine = create_mysql_engine(self.resolved_db_uri)

            def start_bundle(self):
                try:
                    self.connection = self.engine.connect()
                except Exception as e:
                    logger.error(f"Failed to get DB connection in start_bundle for {self.table_name}: {e}")
                    raise

            def process(self, row):
                self.buffer.append(row)
                if len(self.buffer) >= 1000:
                    self._write_buffer()

            def finish_bundle(self):
                if self.buffer:
                    self._write_buffer()

            def _write_buffer(self):
                if not self.buffer:
                    return
                try:
                    df = pd.DataFrame(self.buffer)
                    df.to_sql(name=self.table_name, con=self.connection, if_exists='append', index=False, chunksize=500, method='multi')
                    logger.info(f"Wrote {len(self.buffer)} records to {self.table_name}")
                    self.buffer = []
                except Exception as e:
                    logger.error(f"Error writing batch to {self.table_name}: {e}")
                    raise

            def teardown(self):
                if self.connection:
                    self.connection.close()
                    self.connection = None
                if self.engine:
                    self.engine.dispose()
                    self.engine = None
                self.buffer = []

        purchases_pcoll = (
            pipeline
            | "StartPurchases" >> beam.Create([None])
            | "ReadPurchases" >> beam.ParDo(ReadPurchases(input_db_uri, process_all_flag))
            | "KeyPurchasesByPONumber" >> beam.Map(lambda r: (r['PONumber'], r))
        )

        invoices_pcoll = (
            pipeline
            | "StartInvoices" >> beam.Create([None])
            | "ReadInvoices" >> beam.ParDo(ReadInvoices(input_db_uri, process_all_flag))
            | "KeyInvoicesByPONumber" >> beam.Map(lambda r: (r['PONumber'], r))
        )

        class ProcessJoinedPurchasesInvoices(beam.DoFn):
            def process(self, element):
                ponumber, tagged_elements = element
                purchase_records = tagged_elements['purchases'] # Using 'purchases' as the tag
                invoice_records = tagged_elements['invoices']  # Using 'invoices' as the tag

                now = datetime.now()
                
                invoice_data = {}
                if invoice_records: # invoices_records will be a list
                    invoice_data = invoice_records[0] # Assuming at most one invoice per PONumber

                for purchase_record in purchase_records: # purchase_records will be a list
                    merged = {
                        'Store': purchase_record.get('Store'),
                        'VendorNumber': purchase_record.get('VendorNumber'),
                        'PONumber': ponumber,
                        'PODate': convert_date(purchase_record.get('PODate')),
                        'InvoiceDate': convert_date(invoice_data.get('InvoiceDate', purchase_record.get('InvoiceDate'))),
                        'PayDate': convert_date(invoice_data.get('PayDate', purchase_record.get('PayDate'))),
                        'PurchasePrice': purchase_record.get('PurchasePrice'),
                        'Quantity': purchase_record.get('Quantity'),
                        'Dollars': purchase_record.get('Dollars'),
                        'Freight': invoice_data.get('Freight'),
                        'fecha_actualizacion': now,
                        'Description': purchase_record.get('Description')
                    }
                    yield merged

        joined_pcoll = (
            {'purchases': purchases_pcoll, 'invoices': invoices_pcoll}
            | "CoGroupPurchasesInvoices" >> beam.CoGroupByKey()
            | "ProcessJoinedData" >> beam.ParDo(ProcessJoinedPurchasesInvoices())
        )

        return (
            joined_pcoll
            | "WritePurchases" >> beam.ParDo(WritePurchasesToMySQL(output_db_uri, "purchase"))
        )

# ==================== ETL PARA INVENTORY END ====================
class InventoryEndETL:
    @staticmethod
    def execute(pipeline, options, product_data_pcoll_input=None):
        input_db_uri = options.input_db_uri
        output_db_uri = options.output_db_uri
        process_all_flag = options.process_all

        class ReadBegInv(beam.DoFn):
            def __init__(self, db_uri, process_all):
                self.db_uri = db_uri
                self.process_all = process_all
            
            def process(self, element):
                db_uri = self.db_uri.get()
                process_all = self.process_all.get().lower() == 'true'
                for record in read_table(db_uri, 'BegInvFINAL12312016', None, process_all): # Fixed variable names
                    # Punto de inspección:
                    logger.info(f"ReadBegInv yielding record of type: {type(record)}, value: {str(record)[:200]}") # Loguear tipo y parte del valor
                    # Ensure the yielded record is a dictionary before processing
                    if isinstance(record, dict):
                        if not record.get('PurchasePrice') or record.get('PurchasePrice') == 0:
                            continue
                    else:
                        logger.warning(f"ReadBegInv (in InventoryEndETL) received non-dict record of type: {type(record)}, value: {str(record)[:200]}. Skipping.")
                    record['Description'] = clean_string(record.get('Description'))
                    yield record

        class ReadEndInv(beam.DoFn):
            def __init__(self, db_uri, process_all):
                self.db_uri = db_uri
                self.process_all = process_all

            def process(self, element):
                db_uri = self.db_uri.get()
                process_all = self.process_all.get().lower() == 'true'
                for record in read_table(db_uri, 'EndInvFINAL12312016', None, process_all):
                    if record.get('endDate') is None or record.get('Store') is None:
                        continue
                    yield record

        class CleanEndInvData(beam.DoFn):
            # __init__ is no longer needed for this side input, or can be an empty def __init__(self): pass

            def process(self, record, store_to_city_map): # Side input passed as argument
                city = record.get('City')
                store = record.get('Store')
                if (city is None or (isinstance(city, str) and city.strip() == "")) and store in store_to_city_map:
                    record['City'] = store_to_city_map[store]
                inventory_id = record.get('InventoryId')
                if inventory_id and '__' in inventory_id:
                    city_val = record.get('City')
                    if city_val:
                        record['InventoryId'] = inventory_id.replace('_', f"{city_val}_")
                record['City'] = clean_string(record.get('City'))
                record['Description'] = clean_string(record.get('Description'))
                record['endDate'] = convert_date(record.get('endDate'))
                yield record

        def join_endinv_product(endinv, products_dict):
            # Asegurar que 'endinv' sea un diccionario
            if not isinstance(endinv, dict):
                logger.warning(f"join_endinv_product (InventoryEndETL) recibió 'endinv' que no es un diccionario. Tipo: {type(endinv)}. Saltando elemento.")
                return [] # FlatMap espera un iterable; lista vacía para descartar

            desc_norm = normalize_desc(endinv.get('Description'))
            product = products_dict.get(desc_norm)

            product_id_to_assign = None
            if product and isinstance(product, dict):
                product_id_to_assign = product.get('ProductID')
            elif product: # product existe pero no es un diccionario
                logger.warning(f"join_endinv_product (InventoryEndETL): El producto encontrado para la descripción '{desc_norm}' no es un diccionario. Tipo: {type(product)}. No se puede obtener ProductID.")

            endinv['ProductID'] = product_id_to_assign
            return [endinv] # Retornar como una lista para FlatMap

        class WriteInventoryEndToMySQL(beam.DoFn):
            def __init__(self, db_uri, table_name):
                self.db_uri_vp = db_uri
                self.table_name = table_name
                self.buffer = []
                self.engine = None
                self.connection = None
                self.resolved_db_uri = None

            def setup(self):
                self.resolved_db_uri = self.db_uri_vp.get()
                self.engine = create_mysql_engine(self.resolved_db_uri)

            def start_bundle(self):
                self.buffer = []
                try:
                    self.connection = self.engine.connect()
                except Exception as e:
                    logger.error(f"Failed to get DB connection in start_bundle for {self.table_name}: {e}")
                    raise

            def process(self, record):
                self.buffer.append(record)
                if len(self.buffer) >= 1000:
                    self._write_buffer()

            def finish_bundle(self):
                if self.buffer:
                    self._write_buffer()

            def _write_buffer(self):
                if not self.buffer:
                    return
                try:
                    df = pd.DataFrame(self.buffer)
                    df.to_sql(name=self.table_name, con=self.connection, if_exists='append', index=False, chunksize=500, method='multi')
                    logger.info(f"Wrote {len(self.buffer)} records to {self.table_name}")
                    self.buffer = []
                except Exception as e:
                    logger.error(f"Error writing batch to {self.table_name}: {e}")
                    raise

            def teardown(self):
                if self.connection:
                    self.connection.close()
                    self.connection = None
                if self.engine:
                    self.engine.dispose()
                    self.engine = None
                self.buffer = []

        store_city_pairs = (
            pipeline
            | "StartStoreCities" >> beam.Create([None])
            | "ReadStoreCities" >> beam.ParDo(ReadBegInv(input_db_uri, process_all_flag))
            | "DistinctStoreCities" >> beam.Distinct()
            | "ToDictStoreCities" >> beam.combiners.ToDict(lambda x: (x['Store'], x['City']))
        )

        endinv = (
            pipeline
            | "StartEndInv" >> beam.Create([None])
            | "ReadEndInv" >> beam.ParDo(ReadEndInv(input_db_uri, process_all_flag))
        )

        if product_data_pcoll_input is None:
            logger.warning("product_data_pcoll_input not provided to InventoryEndETL. Falling back to DB read for products, which may cause timing issues.")
            class ReadProductForEndInv_Fallback(beam.DoFn):
                def __init__(self, db_uri_vp):
                    self.db_uri_vp = db_uri_vp
                def process(self, element):
                    for record in read_table(self.db_uri_vp.get(), "product", None, True):
                        yield record
            products_source = (
                pipeline
                | "StartProducts_End_Fallback" >> beam.Create([None])
                | "ReadProducts_2_Fallback" >> beam.ParDo(ReadProductForEndInv_Fallback(output_db_uri))
            )
        else:
            products_source = product_data_pcoll_input
            
        products_dict_pcoll = (
            products_source # Aplicar las transformaciones a la PCollection de entrada
            | "MapProductsToKV_EndInv" >> beam.Map(lambda x: (normalize_desc(x.get('Description',"")), x))
            | "ToDictProducts_1" >> beam.combiners.ToDict()
        )

        return (
            endinv
            | "CleanEndInv" >> beam.ParDo(CleanEndInvData(), store_to_city_map=beam.pvalue.AsSingleton(store_city_pairs))
            | "JoinWithProducts_1" >> beam.FlatMap(
                join_endinv_product, 
                products_dict=beam.pvalue.AsSingleton(products_dict_pcoll))
            | "SelectFields_1" >> beam.Map(lambda r: {
                'Store': r.get('Store'),
                'ProductID': r.get('ProductID'),
                'onHand': r.get('onHand'),
                'endDate': r.get('endDate'),
                'fecha_actualizacion': datetime.now()
            })
            # Removed steps that generated InventoryEndID
            | "WriteInventoryEnd" >> beam.ParDo(WriteInventoryEndToMySQL(output_db_uri, "InventoryEnd"))
        )

# ==================== ETL PARA INVENTORY BEG ====================
class InventoryBegETL:
    @staticmethod
    def execute(pipeline, options, product_data_pcoll_input=None):
        input_db_uri = options.input_db_uri
        output_db_uri = options.output_db_uri
        process_all_flag = options.process_all

        class CleanBegInvData(beam.DoFn):
            def process(self, record):
                desc = record.get('Description', '')
                if desc and re.search('Gerard Bertrand Organic Rose', desc, re.IGNORECASE):
                    record['Price'] = 9.99

                record['City'] = clean_string(record.get('City'))
                record['Description'] = clean_string(record.get('Description'))

                record['startDate'] = convert_date(record.get('startDate'))
                if record['startDate'] is None:
                    return

                yield record

        class ReadBegInvForBegInv(beam.DoFn):
            def __init__(self, db_uri, process_all):
                self.db_uri = db_uri
                self.process_all = process_all

            def process(self, element):
                db_uri = self.db_uri.get()
                process_all = self.process_all.get().lower() == 'true'
                for record in read_table(db_uri, "BegInvFINAL12312016", None, process_all):
                    # Ensure the yielded record is a dictionary
                    if isinstance(record, dict):
                        yield record
                    else:
                        logger.warning(f"ReadBegInv (in InventoryBegETL) received non-dict record of type: {type(record)}, value: {str(record)[:200]}. Skipping.")


        def join_beginv_product(beginv, products_dict):
            # Asegurar que 'beginv' sea un diccionario
            if not isinstance(beginv, dict):
                logger.warning(f"join_beginv_product (InventoryBegETL) recibió 'beginv' que no es un diccionario. Tipo: {type(beginv)}. Saltando elemento.")
                return [] # FlatMap espera un iterable; lista vacía para descartar

            desc_norm = normalize_desc(beginv.get('Description'))
            product = products_dict.get(desc_norm)

            product_id_to_assign = None
            if product and isinstance(product, dict):
                product_id_to_assign = product.get('ProductID')
            elif product: # product existe pero no es un diccionario
                logger.warning(f"join_beginv_product (InventoryBegETL): El producto encontrado para la descripción '{desc_norm}' no es un diccionario. Tipo: {type(product)}. No se puede obtener ProductID.")

            beginv['ProductID'] = product_id_to_assign
            return [beginv] # Retornar como una lista para FlatMap

        class WriteInventoryBegToMySQL(beam.DoFn):
            def __init__(self, db_uri, table_name):
                self.db_uri_vp = db_uri
                self.table_name = table_name
                self.buffer = []
                self.engine = None
                self.connection = None
                self.resolved_db_uri = None

            def setup(self):
                self.resolved_db_uri = self.db_uri_vp.get()
                self.engine = create_mysql_engine(self.resolved_db_uri)

            def start_bundle(self):
                self.buffer = []
                try:
                    self.connection = self.engine.connect()
                except Exception as e:
                    logger.error(f"Failed to get DB connection in start_bundle for {self.table_name}: {e}")
                    raise

            def process(self, record):
                self.buffer.append(record)
                if len(self.buffer) >= 1000:
                    self._write_buffer()

            def finish_bundle(self):
                if self.buffer:
                    self._write_buffer()

            def _write_buffer(self):
                if not self.buffer:
                    return
                try:
                    df = pd.DataFrame(self.buffer)
                    df.to_sql(name=self.table_name, con=self.connection, if_exists='append', index=False, chunksize=500, method='multi')
                    logger.info(f"Wrote {len(self.buffer)} records to {self.table_name}")
                    self.buffer = []
                except Exception as e:
                    logger.error(f"Error writing batch to {self.table_name}: {e}")
                    raise

            def teardown(self):
                if self.connection:
                    self.connection.close()
                    self.connection = None
                if self.engine:
                    self.engine.dispose()
                    self.engine = None
                self.buffer = []

        beginv = (
            pipeline
            | "StartBegInv_Beg" >> beam.Create([None])
            | "ReadBegInv_2" >> beam.ParDo(ReadBegInvForBegInv(input_db_uri, process_all_flag))
            | "CleanBegInv" >> beam.ParDo(CleanBegInvData())
        )

        if product_data_pcoll_input is None:
            logger.warning("product_data_pcoll_input not provided to InventoryBegETL. Falling back to DB read for products, which may cause timing issues.")
            class ReadProductForBegInv_Fallback(beam.DoFn):
                def __init__(self, db_uri_vp):
                    self.db_uri_vp = db_uri_vp
                def process(self, element):
                    for record in read_table(self.db_uri_vp.get(), "product", None, True):
                        yield record
            products_source = (
                pipeline
                | "StartProducts_Beg_Fallback" >> beam.Create([None])
                | "ReadProducts_1_Fallback" >> beam.ParDo(ReadProductForBegInv_Fallback(output_db_uri))
            )
        else:
            products_source = product_data_pcoll_input

        products_dict_pcoll = (
            products_source
            | "ToKeyValueProducts" >> beam.Map(lambda x: (normalize_desc(x.get('Description',"")), x))
            | "ToDictProducts_2" >> beam.combiners.ToDict()
        )

        return (
            beginv
            | "JoinWithProducts_2" >> beam.FlatMap(join_beginv_product, products_dict=beam.pvalue.AsSingleton(products_dict_pcoll))
            | "SelectFields_2" >> beam.Map(lambda r: {
                'Store': r.get('Store'),
                'ProductID': r.get('ProductID'),
                'onHand': r.get('onHand'),
                'startDate': r.get('startDate'),
                'fecha_actualizacion': datetime.now()
            })
            # Removed steps that generated InventoryBegID
            | "WriteInventoryBeg" >> beam.ParDo(WriteInventoryBegToMySQL(output_db_uri, "InventoryBeg"))
        )

# ==================== ETL PARA DIMENSIONES ====================
class DimensionsETL:
    @staticmethod
    def execute(pipeline, options):
        input_db_uri = options.input_db_uri
        output_db_uri = options.output_db_uri

        def generate_calendar_records(min_date_in, max_date_in):
            # Asegurar que min_date_in y max_date_in sean objetos datetime
            current = convert_date(min_date_in)
            max_d = convert_date(max_date_in)

            if current is None or max_d is None:
                logger.warning(f"No se pueden generar registros de calendario debido a fechas mín/máx inválidas después de la conversión: min='{min_date_in}', max='{max_date_in}'")
                return
            calendar_id = 1
            today = dt.datetime.now() # Usar dt.datetime para consistencia

            while current <= max_d:
                yield {
                    'CalendarID': calendar_id,
                    'Fecha': current, # Será dt.datetime si se parseó desde cadena,
                    'Año': current.year,
                    'Mes': current.strftime('%B'),
                    'NumeroMes': current.month,
                    'fecha_actualizacion': today
                }
                calendar_id += 1
                current += dt.timedelta(days=1)

        def fetch_min_max_dates(uri):
            engine = create_engine(uri)
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MIN(min_date) AS min_date, MAX(max_date) AS max_date FROM (
                        SELECT MIN(SalesDate) AS min_date, MAX(SalesDate) AS max_date FROM SalesFINAL12312016
                        UNION ALL
                        SELECT MIN(PODate) AS min_date, MAX(PODate) AS max_date FROM PurchasesFINAL12312016
                        UNION ALL
                        SELECT MIN(InvoiceDate) AS min_date, MAX(InvoiceDate) AS max_date FROM InvoicePurchases12312016
                        UNION ALL
                        SELECT MIN(startDate) AS min_date, MAX(startDate) AS max_date FROM BegInvFINAL12312016
                        UNION ALL
                        SELECT MIN(endDate) AS min_date, MAX(endDate) AS max_date FROM EndInvFINAL12312016
                    ) subquery
                """)).first()
                if result:
                    return result[0], result[1] # Acceder por índice
                else:
                    return None, None # Manejar el caso donde no hay resultados
            
            
        class ReadTableViaDoFn(beam.DoFn):
            def __init__(self, db_uri_vp, table_name_str, log_table_str_or_none, process_all_bool):
                self._db_uri_vp = db_uri_vp
                self._table_name_str = table_name_str
                self._log_table_str_or_none = log_table_str_or_none
                self._process_all_bool = process_all_bool

            def process(self, element): # element is from Create([None])
                db_uri_resolved = self._db_uri_vp.get()
                for record in read_table(db_uri_resolved, self._table_name_str, 
                                         self._log_table_str_or_none, self._process_all_bool):
                    if isinstance(record, dict): # Añadir verificación de tipo
                        yield record
                    else:
                        logger.warning(f"ReadTableViaDoFn (table: {self._table_name_str}) received non-dict record: {type(record)}. Skipping.")

        class WriteToMySQLBuffered(beam.DoFn):
            def __init__(self, db_uri, table_name, batch_size=1000):
                self.db_uri_vp = db_uri 
                self.table_name = table_name # This is a string, not a ValueProvider
                self.batch_size = batch_size
                self.buffer = []
                self.engine = None
                self.connection = None
                self.resolved_db_uri = None

            def setup(self):
                self.resolved_db_uri = self.db_uri_vp.get()
                self.engine = create_mysql_engine(self.resolved_db_uri)

            def start_bundle(self):
                self.buffer = []
                try:
                    self.connection = self.engine.connect()
                except Exception as e:
                    logger.error(f"Failed to get DB connection in start_bundle for {self.table_name}: {e}")
                    raise

            def process(self, record):
                self.buffer.append(record)
                if len(self.buffer) >= self.batch_size:
                    self._write_buffer()

            def finish_bundle(self):
                if self.buffer:
                    self._write_buffer()

            def _write_buffer(self):
                if not self.buffer:
                    return
                try:
                    df = pd.DataFrame(self.buffer)
                    df.to_sql(name=self.table_name, con=self.connection, if_exists='append', index=False, chunksize=500, method='multi')
                    logger.info(f"Wrote {len(self.buffer)} records to {self.table_name}")
                    self.buffer = []
                except Exception as e:
                    logger.error(f"Error writing batch to {self.table_name}: {e}")
                    raise

            def teardown(self):
                if self.connection:
                    self.connection.close()
                    self.connection = None
                if self.engine:
                    self.engine.dispose()
                    self.engine = None
                self.buffer = []

        stores_pipeline = (
            pipeline
            | "StartStores" >> beam.Create([None])
            | "ReadStores" >> beam.ParDo(
                ReadTableViaDoFn(input_db_uri, "EndInvFINAL12312016", None, True))
            # Extraer solo los campos para la unicidad
            | "ExtractStoreFieldsForDistinct" >> beam.Map(lambda r: (r.get('Store'), r.get('City')))
            | "DistinctStores" >> beam.Distinct()
            # Volver a convertir a diccionario y añadir fecha_actualizacion
            | "ReconstructStoreDict" >> beam.Map(lambda t: {
                'Store': t[0],
                'City': t[1],
                'fecha_actualizacion': datetime.now()
            })
            | "WriteStores" >> beam.ParDo(WriteToMySQLBuffered(output_db_uri, "store"))
        )

        vendors_pipeline = (
            pipeline
            | "StartVendors" >> beam.Create([None])
            | "ReadVendors" >> beam.ParDo(
                ReadTableViaDoFn(input_db_uri, "InvoicePurchases12312016", None, True))
            # Extraer solo los campos para la unicidad
            | "ExtractVendorFieldsForDistinct" >> beam.Map(lambda r: (r.get('VendorNumber'), r.get('VendorName')))
            | "DistinctVendors" >> beam.Distinct()
            # Volver a convertir a diccionario y añadir fecha_actualizacion
            | "ReconstructVendorDict" >> beam.Map(lambda t: {
                'VendorNumber': t[0],
                'VendorName': t[1],
                'fecha_actualizacion': datetime.now()
            })
            | "WriteVendors" >> beam.ParDo(WriteToMySQLBuffered(output_db_uri, "vendor"))
        )

        calendar_pipeline = (
            pipeline
            | "CreateForCalendar" >> beam.Create([None]) # Start the PCollection
            # Resolve the ValueProvider within the Map's lambda
            | "FetchDates" >> beam.Map(lambda _, input_db_uri_vp: fetch_min_max_dates(input_db_uri_vp.get()), input_db_uri_vp=input_db_uri)
            | "GenerateCalendar" >> beam.FlatMap(lambda dates:
                generate_calendar_records(dates[0], dates[1]) if dates[0] and dates[1] else []
            )
            | "WriteCalendar" >> beam.ParDo(WriteToMySQLBuffered(output_db_uri, "calendar"))
        )

        return (stores_pipeline, vendors_pipeline, calendar_pipeline)


def get_tablas_detectadas_fuera(input_db_uri, log_table, process_all_flag):
    input_db_uri_value = input_db_uri.get() if hasattr(input_db_uri, 'get') else input_db_uri
    log_table_value = log_table.get() if hasattr(log_table, 'get') else log_table
    process_all_value = str(process_all_flag.get() if hasattr(process_all_flag, 'get') else process_all_flag).lower() == 'true'

    engine = create_engine(input_db_uri_value)
    tables_to_process = []

    try:
        with engine.connect() as conn:
            if process_all_value:
                tables_to_process = [
                    'SalesFINAL12312016', 'PurchasesFINAL12312016',
                    'BegInvFINAL12312016', 'EndInvFINAL12312016',
                    'PurchasePricesDec2017', 'InvoicePurchases12312016'
                ]
            else:
                query_logs = text(f"SELECT tabla_origen, ultima_fecha FROM {log_table_value}")
                log_results = conn.execute(query_logs).mappings().all()

                for row in log_results:
                    tabla = row['tabla_origen']
                    ultima_fecha = row['ultima_fecha']

                    query_max_fecha = text(f"SELECT MAX(fecha_actualizacion) as max_fecha FROM {tabla}")
                    max_fecha_result = conn.execute(query_max_fecha).mappings().fetchone()
                    max_fecha = max_fecha_result['max_fecha'] if max_fecha_result else None

                    if max_fecha and max_fecha > ultima_fecha:
                        tables_to_process.append(tabla)

    except Exception as e:
        logger.error(f"Error al consultar tabla de logs: {e}")
        raise

    return tables_to_process


def run():
    options = PipelineOptions()
    custom_options = options.view_as(CustomPipelineOptions)
    # Resolver process_all para la lógica condicional en run()
    # Usamos el mismo patrón robusto que en get_tablas_detectadas_fuera
    process_all_value_for_run = str(
        custom_options.process_all.get() if hasattr(custom_options.process_all, 'get') 
        else custom_options.process_all
    ).lower() == 'true'

    # Detectar tablas usando la función correcta
    tablas_detectadas = get_tablas_detectadas_fuera(
        custom_options.input_db_uri,
        custom_options.log_table,
        custom_options.process_all
    )
    if not tablas_detectadas and not process_all_value_for_run:
        logger.info("No se detectaron cambios en las tablas de origen y process_all es falso. No se ejecutarán los ETLs de datos.")


    with beam.Pipeline(options=options) as p:
            product_pcollection_for_side_input = None
            # Determinar qué ETLs principales se ejecutarán, considerando process_all_value_for_run
            needs_sales_etl = process_all_value_for_run or ('SalesFINAL12312016' in tablas_detectadas)
            needs_purchases_etl = process_all_value_for_run or any(t in tablas_detectadas for t in ['PurchasesFINAL12312016', 'InvoicePurchases12312016'])
            
            # PurchasePriceBegInvETL es el productor de la tabla 'product'
            # Sus fuentes son PurchasePricesDec2017 y BegInvFINAL12312016 (para el join)
            producer_sources_updated = process_all_value_for_run or any(t in tablas_detectadas for t in ['PurchasePricesDec2017', 'BegInvFINAL12312016'])
            
            # InventoryBegETL y InventoryEndETL son consumidores de 'product'
            # Sus fuentes directas son BegInvFINAL12312016 y EndInvFINAL12312016 respectivamente
            needs_inventory_beg_etl_sources = process_all_value_for_run or ('BegInvFINAL12312016' in tablas_detectadas)
            needs_inventory_end_etl_sources = process_all_value_for_run or ('EndInvFINAL12312016' in tablas_detectadas)

            # PurchasePriceBegInvETL debe ejecutarse si sus fuentes se actualizan
            # O si alguno de sus consumidores (InventoryBegETL o InventoryEndETL) necesita ejecutarse.
            needs_product_producer_etl = producer_sources_updated or needs_inventory_beg_etl_sources or needs_inventory_end_etl_sources
            
            # Ejecución de ETLs
            if needs_sales_etl:
                SalesETL.execute(p, custom_options)
            if needs_purchases_etl:
                PurchasesETL.execute(p, custom_options)
            if needs_product_producer_etl:
                product_pcollection_for_side_input = PurchasePriceBegInvETL.execute(p, custom_options)
            if needs_inventory_beg_etl_sources:
                InventoryBegETL.execute(p, custom_options, product_data_pcoll_input=product_pcollection_for_side_input)
            if needs_inventory_end_etl_sources:
                InventoryEndETL.execute(p, custom_options, product_data_pcoll_input=product_pcollection_for_side_input)
            # Ejecutar DimensionsETL si process_all es true o si alguna tabla fue detectada para procesamiento
            if process_all_value_for_run or tablas_detectadas:
                DimensionsETL.execute(p, custom_options)

    with beam.Pipeline(options=options) as p2:
        _ = (
            p2
            | "CrearListaParaLogs" >> beam.Create(tablas_detectadas)
            | "ActualizarLogs" >> beam.ParDo(UpdateLogDoFn(
                custom_options.input_db_uri,
                custom_options.log_table))
        )


if __name__ == "__main__":
    run()
