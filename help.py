from datetime import datetime

import import as F
import pyspark.sql.functions
from pyspark.sql.types import LongType, StructField, StructType
from utils.update_datacatalog import update_table_data_catalog


def escrita_dados(
    resultado_df,
    numero_lote_spec,
    logger,
    glue_client_role,
    table_name_spec,
    lake_control_account_id,
    s3_client):

    partition_col = datetime.now().strftime("%Y%m%d")
    resultado_df = atribui_num_lote(resultado_df, numero_lote_spec)

    location, storage_descriptor = get_table_location_storage_descriptor(
        glue_client_role,
        database_name_spec,
        table_name_spec,
        lake_control_account_id
    )

    write_to_s3(
        resultado_df,
        location, 
        partition_col, 
        logger
    )

    update_table_data_catalog(
        storage_descriptor,
        logger,
        glue_client_role,
        database_name_spec,
        table_name_spec,
        lake_control_account_id,
        s3_client
    )

def get_table_location_storage_descriptor(
    glue_client_role,
    database_name_spec,
    table_name_spec,
    lake_control_account_id):

    storage_descriptor = glue_client_role.get_table(
        DatabaseName=database_name_spec,
        Name=table_name_spec,
        CatalogId=lake_control_account_id
    )['Table']['StorageDescriptor']
    location = storage_descriptor['Location']

    return location, storage_descriptor

def write_to_s3(resultado_df, location, partition_col, logger):
    for i in range(3):
        try:
            (
                resultado_df
                .write.format('json')
                .partitionBy('num_lote')
                .option('maxRecordsPerFile', 1)
                .mode('overwrite')
                .save(f"{location}/num_ano_mes_dia={partition_col}/")
            )
            break
        except Exception as e:
            if i == 2:
                raise e
            else:
                logger.warning(f"Erro na escrita {i}/2. Erro {e}")

def atribui_num_lote(resultado_df, numero_lote_spec):
    new_schema = StructType([StructField('index', LongType(), False)] + resultado_df.schema.fields[:])
    resultado_df = (
        resultado_df.rdd.zipWithIndex()
        .map(lambda x: (x[1] + 1,) + x[0])
        .taDF(schema=new_schema))
    resultado_df = resultado_df.withColumn(
        'num_lote',
        ((F.col('index')/ numero_lote_spec)
         .cast('int')+ 1)).drop('index')

    return resultado_df
