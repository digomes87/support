from datetime import datetime
from typing import Any, Tuple, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, StructField, StructType
from utils.update_datacatalog import update_table_data_catalog
from utils.logging_config import AWSServiceError, DataProcessingError


def escrita_dados(
    resultado_df: DataFrame,
    numero_lote_spec: int,
    logger: Any,
    glue_client_role: Any,
    database_name_spec: str,
    table_name_spec: str,
    lake_control_account_id: str,
    s3_client: Any,
    s3_bucket_name: Optional[str] = None,
    s3_bucket_prefix: Optional[str] = None) -> None:
    """Write data to S3 and update data catalog with comprehensive error handling."""
    try:
        # Validate input parameters
        if resultado_df is None or resultado_df.count() == 0:
            raise DataProcessingError("Empty or null DataFrame provided", "data_validation", 
                                    {"table": table_name_spec})
        
        if numero_lote_spec <= 0:
            raise DataProcessingError("Invalid batch number", "parameter_validation", 
                                    {"numero_lote_spec": numero_lote_spec})

        logger.log_operation_start("Batch Number Assignment", batch_size=numero_lote_spec)
        
        # Assign batch number
        try:
            partition_col = datetime.now().strftime("%Y%m%d")
            resultado_df = atribui_num_lote(resultado_df, numero_lote_spec)
            logger.log_operation_end("Batch Number Assignment")
        except Exception as e:
            raise DataProcessingError("Failed to assign batch numbers", "batch_assignment", 
                                    {"error": str(e), "batch_size": numero_lote_spec})

        # Get table location and storage descriptor
        try:
            logger.log_operation_start("Table Metadata Retrieval", 
                                     database=database_name_spec, 
                                     table=table_name_spec)
            location, storage_descriptor = get_table_location_storage_descriptor(
                glue_client_role,
                database_name_spec,
                table_name_spec,
                lake_control_account_id
            )
            logger.log_aws_operation("Glue", "Get Table Metadata", "SUCCESS", 
                                   table=table_name_spec, 
                                   location=location)
        except Exception as e:
            raise AWSServiceError("Failed to retrieve table metadata", "glue_metadata", 
                                {"database": database_name_spec, "table": table_name_spec, "error": str(e)})

        # Write to S3
        try:
            logger.log_operation_start("S3 Data Write", 
                                     location=location, 
                                     partition_column=partition_col)
            write_to_s3(
                resultado_df,
                location, 
                partition_col, 
                logger
            )
            logger.log_aws_operation("S3", "Data Write", "SUCCESS", 
                                   location=location, 
                                   partition=f"num_ano_mes_dia={partition_col}")
        except Exception as e:
            raise AWSServiceError("Failed to write data to S3", "s3_write", 
                                {"location": location, "error": str(e)})

        # Update data catalog
        try:
            logger.log_operation_start("Data Catalog Update", 
                                     database=database_name_spec, 
                                     table=table_name_spec)
            update_table_data_catalog(
                partition_col,
                storage_descriptor,
                logger,
                glue_client_role,
                database_name_spec,
                table_name_spec,
                lake_control_account_id,
                s3_client
            )
            logger.log_aws_operation("Glue", "Update Data Catalog", "SUCCESS", 
                                   table=table_name_spec, 
                                   partition=f"num_ano_mes_dia={partition_col}")
        except Exception as e:
            raise AWSServiceError("Failed to update data catalog", "catalog_update", 
                                {"database": database_name_spec, "table": table_name_spec, "error": str(e)})

        logger.log_operation_end("Data Writing Process")
        
    except (DataProcessingError, AWSServiceError) as e:
        logger.log_error_with_context(e, e.operation, **e.context)
        raise
    except Exception as e:
        logger.log_error_with_context(e, "data_writing_unknown_error", 
                                    table=table_name_spec, 
                                    database=database_name_spec)
        raise DataProcessingError("Unexpected error during data writing", "data_writing_unknown_error", 
                                {"table": table_name_spec, "error": str(e)})

def get_table_location_storage_descriptor(
    glue_client_role: Any,
    database_name_spec: str,
    table_name_spec: str,
    lake_control_account_id: str) -> Tuple[str, Any]:

    storage_descriptor = glue_client_role.get_table(
        DatabaseName=database_name_spec,
        Name=table_name_spec,
        CatalogId=lake_control_account_id
    )['Table']['StorageDescriptor']
    location = storage_descriptor['Location']

    return location, storage_descriptor

def write_to_s3(resultado_df: DataFrame, location: str, partition_col: str, logger: Any) -> None:
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

def atribui_num_lote(resultado_df: DataFrame, numero_lote_spec: int) -> DataFrame:
    new_schema = StructType([StructField('index', LongType(), False)] + resultado_df.schema.fields[:])
    resultado_df = (
        resultado_df.rdd.zipWithIndex()
        .map(lambda x: (x[1],) + x[0])
        .toDF(schema=new_schema))
    resultado_df = resultado_df.withColumn(
        'num_lote',
        ((F.col('index') / numero_lote_spec)
         .cast('int') + 1)).drop('index')

    return resultado_df
