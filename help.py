import copy
import re
from datetime import datetime
from logging import Logger

import botocore
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, StructField, StructType


def atribui_num_lote(resultado_df: DataFrame, numero_lote_spec: int) -> DataFrame:
    """Assign batch numbers to DataFrame"""
    new_schemas = StructType([StructField('index', LongType(), False)] + resultado_df.schema.fields[:])
    resultado_df = (resultado_df.rdd.zipWithIndex().map(lambda x: (x[1] + 1,) + x[0]).toDF(schema=new_schemas))
    resultado_df = resultado_df.withColumn('num_lote', ((F.col('index') / numero_lote_spec).cast('int') + 1)).drop('index')
    return resultado_df


def get_lista_particoes_spec_from_catalog(
        num_ano_mes_dia: str,
        glue_client_role,
        database_name_spec: str,
        table_name_spec: str,
        lake_control_account_id: str
        ):
    """Get partition list from Glue catalog"""
    list_particoes = []

    partition_info = glue_client_role.get_partitions(
        DatabaseName=database_name_spec,
        TableName=table_name_spec,
        ExcludeColumnSchema=True,
        Expression=f"num_ano_mes_dia={num_ano_mes_dia}",
        CatalogId=lake_control_account_id,
    )

    while True:
        if partition_info.get("Partitions"):
            for partition in partition_info["Partitions"]:
                if len(partition['Values']) != 0:
                    list_particoes.append({'Values': partition['Values']})

        next_token = partition_info.get("NextToken")
        if not next_token:
            break

        partition_info = glue_client_role.get_partitions(
            DatabaseName=database_name_spec,
            TableName=table_name_spec,
            ExcludeColumnSchema=True,
            NextToken=next_token,
            Expression=f"num_ano_mes_dia={num_ano_mes_dia}",
            CatalogId=lake_control_account_id,
        )

    return list_particoes


def escrita_dados(
    resultado_df: DataFrame,
    numero_lote_spec: int,
    logger: Logger,
    glue_client_role,
    database_name_spec: str,
    table_name_spec: str,
    lake_control_account_id: str,
    s3_client,
    s3_bucket_name: str = None,
    s3_bucket_prefix: str = None) -> None:
    """Write data to S3 and update data catalog"""
    
    try:
        logger.info("Starting data writing process")
        
        # Validate input parameters
        if resultado_df is None or resultado_df.count() == 0:
            logger.error("Empty or null DataFrame provided")
            return
        
        if numero_lote_spec <= 0:
            logger.error(f"Invalid batch number: {numero_lote_spec}")
            return

        # Assign batch number
        partition_col = datetime.now().strftime("%Y%m%d")
        resultado_df = atribui_num_lote(resultado_df, numero_lote_spec)
        
        # Get table location
        table_info = glue_client_role.get_table(
            DatabaseName=database_name_spec,
            Name=table_name_spec,
            CatalogId=lake_control_account_id
        )
        
        location = table_info['Table']['StorageDescriptor']['Location']
        
        # Write to S3
        write_to_s3(resultado_df, location, partition_col, logger)
        
        logger.info("Data writing process completed successfully")
        
    except Exception as e:
        logger.error(f"Error in data writing process: {str(e)}")
        raise


def write_to_s3(resultado_df: DataFrame, location: str, partition_col: str, logger: Logger) -> None:
    """Write DataFrame to S3"""
    try:
        logger.info(f"Writing data to S3 location: {location}")
        
        resultado_df.write \
            .mode("append") \
            .partitionBy("num_ano_mes_dia") \
            .parquet(f"{location}/num_ano_mes_dia={partition_col}")
            
        logger.info("Successfully wrote data to S3")
        
    except Exception as e:
        logger.error(f"Error writing to S3: {str(e)}")
        raise
