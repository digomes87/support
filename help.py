from datetime import datetime
from logging import Logger

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, StructField, StructType


def atribui_num_lote(resultado_df: DataFrame, numero_lote_spec: int) -> DataFrame:
    """Atribui números de lote ao DataFrame"""
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
    """Obtém lista de partições do catálogo Glue"""
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
    """Escreve dados no S3 e atualiza o catálogo de dados"""
    
    try:
        logger.info("Iniciando processo de escrita de dados")
        
        # Valida parâmetros de entrada
        if resultado_df is None or resultado_df.count() == 0:
            logger.error("DataFrame vazio ou nulo fornecido")
            return
        
        if numero_lote_spec <= 0:
            logger.error(f"Número de lote inválido: {numero_lote_spec}")
            return

        # Atribui número de lote
        partition_col = datetime.now().strftime("%Y%m%d")
        resultado_df = atribui_num_lote(resultado_df, numero_lote_spec)
        
        # Obtém localização da tabela
        table_info = glue_client_role.get_table(
            DatabaseName=database_name_spec,
            Name=table_name_spec,
            CatalogId=lake_control_account_id
        )
        
        location = table_info['Table']['StorageDescriptor']['Location']
        
        # Escreve no S3
        write_to_s3(resultado_df, location, partition_col, logger)
        
        logger.info("Processo de escrita de dados concluído com sucesso")
        
    except Exception as e:
        logger.error(f"Erro no processo de escrita de dados: {str(e)}")
        raise


def write_to_s3(resultado_df: DataFrame, location: str, partition_col: str, logger: Logger) -> None:
    """Escreve DataFrame no S3"""
    try:
        logger.info(f"Escrevendo dados na localização S3: {location}")
        
        resultado_df.write \
            .mode("append") \
            .partitionBy("num_ano_mes_dia") \
            .parquet(f"{location}/num_ano_mes_dia={partition_col}")
            
        logger.info("Dados escritos no S3 com sucesso")
        
    except Exception as e:
        logger.error(f"Erro ao escrever no S3: {str(e)}")
        raise
