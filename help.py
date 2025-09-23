"""
Database query functions for data preparation
Based on atividade.md specifications
"""

from logging import Logger
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when

from .database_mapping import DatabaseMappingConfig, build_standardized_query
from .data_validation import get_partition_filter, validate_dataframe_columns, log_dataframe_info


def consulta_tabelas_main(
    spark: SparkSession,
    lista_cpf: List[str],
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Main function to query all tables and combine results
    """
    logger.info("Starting main table queries")
    
    # Initialize database mapping configuration
    db_config = DatabaseMappingConfig()
    
    # Get keyspace queries
    df_keyspace = get_consultas_keyspace(spark, lista_cpf, data_referencia, logger)
    
    # Get Aurora queries
    df_aurora = get_consulta_aurora(spark, lista_cpf, data_referencia, logger)
    
    # Combine results
    resultado_df = df_keyspace.union(df_aurora)
    
    logger.info(f"Combined query result count: {resultado_df.count()}")
    return resultado_df


def get_consultas_keyspace(
    spark: SparkSession,
    lista_cpf: List[str],
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Execute queries for Keyspace tables
    """
    logger.info("Executing Keyspace queries")
    
    db_config = DatabaseMappingConfig()
    keyspace_tables = [
        "conv_cola_info",
        "employee_contract_info", 
        "customer_credit_view",
        "vsao_cred_clie",
        "ctrt_cred_csgd",
        "rend_elto_pfis",
        "grup_vsao_clie_dm"
    ]
    
    dataframes = []
    
    for table_name in keyspace_tables:
        try:
            df = consulta_tabelas_mapped(
                spark, table_name, lista_cpf, data_referencia, logger
            )
            if df is not None:
                dataframes.append(df)
        except Exception as e:
            logger.error(f"Error querying table {table_name}: {str(e)}")
            continue
    
    if dataframes:
        resultado_df = dataframes[0]
        for df in dataframes[1:]:
            resultado_df = resultado_df.union(df)
        return resultado_df
    else:
        # Return empty DataFrame with expected schema
        return spark.createDataFrame([], schema="cod_idef_pess string")


def get_consulta_aurora(
    spark: SparkSession,
    lista_cpf: List[str],
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Execute queries for Aurora tables
    """
    logger.info("Executing Aurora queries")
    
    aurora_tables = [
        "mode_elto_pfis",
        "cred_clie_pfis", 
        "pgto_salr_spi",
        "trsf_cont_salr",
        "dtbc_mode_claf_brau",
        "mode_csgd_crss_dm",
        "crgo_clie_dm",
        "vncl_orgo_pubi_dm",
        "risc_cred_csgb"
    ]
    
    dataframes = []
    
    for table_name in aurora_tables:
        try:
            df = consulta_tabelas_mapped(
                spark, table_name, lista_cpf, data_referencia, logger
            )
            if df is not None:
                dataframes.append(df)
        except Exception as e:
            logger.error(f"Error querying Aurora table {table_name}: {str(e)}")
            continue
    
    if dataframes:
        resultado_df = dataframes[0]
        for df in dataframes[1:]:
            resultado_df = resultado_df.union(df)
        return resultado_df
    else:
        # Return empty DataFrame with expected schema
        return spark.createDataFrame([], schema="cod_idef_pess string")


def consulta_tabelas_mapped(
    spark: SparkSession,
    table_name: str,
    lista_cpf: List[str],
    data_referencia: str,
    logger: Logger
) -> Optional[DataFrame]:
    """
    Query tables using database mapping configuration
    """
    try:
        db_config = DatabaseMappingConfig()
        
        if table_name not in db_config.table_mappings:
            logger.warning(f"Table {table_name} not found in mappings")
            return None
        
        # Build standardized query
        query = build_standardized_query(
            table_name=table_name,
            cpf_list=lista_cpf,
            reference_date=data_referencia
        )
        
        logger.info(f"Executing query for table {table_name}")
        logger.debug(f"Query: {query}")
        
        # Execute query
        df = spark.sql(query)
        
        # Validate and log DataFrame info
        validate_dataframe_columns(df, logger)
        log_dataframe_info(df, table_name, logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Error in consulta_tabelas_mapped for {table_name}: {str(e)}")
        return None


def consulta_tabelas_convenio(
    spark: SparkSession,
    lista_cpf: List[str],
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Query convenio tables with specific logic
    """
    logger.info("Executing convenio queries")
    
    try:
        # Build convenio query
        cpf_condition = "', '".join(lista_cpf)
        
        query = f"""
        SELECT 
            cod_idef_pess,
            numero_convenio,
            nome_convenio,
            data_inicio_convenio,
            data_fim_convenio
        FROM events_convenio
        WHERE cod_idef_pess IN ('{cpf_condition}')
        AND data_referencia = '{data_referencia}'
        """
        
        df = spark.sql(query)
        log_dataframe_info(df, "events_convenio", logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Error in consulta_tabelas_convenio: {str(e)}")
        return spark.createDataFrame([], schema="cod_idef_pess string")


def spark_sql_query(
    spark: SparkSession,
    query: str,
    logger: Logger
) -> Optional[DataFrame]:
    """
    Execute SQL query with error handling
    """
    try:
        logger.debug(f"Executing SQL query: {query}")
        df = spark.sql(query)
        return df
    except Exception as e:
        logger.error(f"Error executing SQL query: {str(e)}")
        return None


def create_data_frame(
    spark: SparkSession,
    database: str,
    table: str,
    logger: Logger
) -> Optional[DataFrame]:
    """
    Create DataFrame from Glue catalog
    """
    try:
        full_table_name = f"{database}.{table}"
        logger.info(f"Creating DataFrame for {full_table_name}")
        
        df = spark.table(full_table_name)
        return df
        
    except Exception as e:
        logger.error(f"Error creating DataFrame for {database}.{table}: {str(e)}")
        return None


def add_missing_columns_with_nulls(
    df: DataFrame,
    expected_columns: List[str],
    logger: Logger
) -> DataFrame:
    """
    Add missing columns with null values
    """
    try:
        existing_columns = df.columns
        missing_columns = [col for col in expected_columns if col not in existing_columns]
        
        if missing_columns:
            logger.info(f"Adding missing columns: {missing_columns}")
            for col_name in missing_columns:
                df = df.withColumn(col_name, lit(None))
        
        return df.select(*expected_columns)
        
    except Exception as e:
        logger.error(f"Error adding missing columns: {str(e)}")
        return df


def get_publico_correntista(
    spark: SparkSession,
    lista_cpf: List[str],
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Get public account holder data
    """
    logger.info("Getting public account holder data")
    
    try:
        cpf_condition = "', '".join(lista_cpf)
        
        query = f"""
        SELECT 
            cod_idef_pess,
            indicador_publico,
            data_inicio_conta,
            data_ultima_movimentacao
        FROM public_person_view
        WHERE cod_idef_pess IN ('{cpf_condition}')
        AND data_referencia = '{data_referencia}'
        """
        
        df = spark.sql(query)
        log_dataframe_info(df, "public_person_view", logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Error in get_publico_correntista: {str(e)}")
        return spark.createDataFrame([], schema="cod_idef_pess string")


def get_convenios(
    spark: SparkSession,
    lista_cpf: List[str],
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Get convenio data with specific business logic
    """
    logger.info("Getting convenio data")
    
    try:
        cpf_condition = "', '".join(lista_cpf)
        
        query = f"""
        SELECT 
            cod_idef_pess,
            numero_convenio,
            tipo_convenio,
            margem_disponivel,
            valor_parcela_atual
        FROM marg_cred
        WHERE cod_idef_pess IN ('{cpf_condition}')
        AND data_referencia = '{data_referencia}'
        AND status_convenio = 'ATIVO'
        """
        
        df = spark.sql(query)
        log_dataframe_info(df, "marg_cred", logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Error in get_convenios: {str(e)}")
        return spark.createDataFrame([], schema="cod_idef_pess string")


def get_agded(
    spark: SparkSession,
    lista_cpf: List[str],
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Get AGDED data with specific transformations
    """
    logger.info("Getting AGDED data")
    
    try:
        cpf_condition = "', '".join(lista_cpf)
        
        query = f"""
        SELECT 
            cod_idef_pess,
            codigo_agencia,
            digito_agencia,
            nome_agencia,
            codigo_produto
        FROM customer_registration_notes
        WHERE cod_idef_pess IN ('{cpf_condition}')
        AND data_referencia = '{data_referencia}'
        """
        
        df = spark.sql(query)
        log_dataframe_info(df, "customer_registration_notes", logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Error in get_agded: {str(e)}")
        return spark.createDataFrame([], schema="cod_idef_pess string")
