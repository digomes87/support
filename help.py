from typing import Optional, List

import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (DecimalType, DoubleType, IntegerType,
                               StringType, TimestampType)
from utils.get_partition_filter import get_partition_filter
from utils.database_mapping import db_mapping, build_standardized_query, get_mapped_query_parts
from utils.logging_config import ETLException, AWSServiceError, DataProcessingError, DataValidationError

spark = SparkSession.builder.getOrCreate()

# defin dados
STRING = StringType()
INTEGER = IntegerType()
DOUBLE = DoubleType()
DECIMAL = DecimalType(10, 2)
TiMESTAMP = TimestampType()


def safe_numeric_cast(field_name, target_type="STRING", alias_name=None):
    """
    Creates a safe cast for numeric fields that might be INT or DOUBLE in Hive.

    Args:
        field_name (str): Name of the field to cast
        target_type (str): Target type for casting ('STRING', 'INT', 'DECIMAL')
        alias_name (str): Alias name for the field (optional)

    Returns:
        str: SQL expression for safe casting
    """
    if alias_name is None:
        alias_name = field_name

    if target_type == "STRING":
        # Convert to INT first to handle DOUBLE, then to STRING
        return f"CAST(CAST({field_name} AS INT) AS STRING) AS {alias_name}"
    elif target_type == "INT":
        # Direct cast to INT (works for both INT and DOUBLE)
        return f"CAST({field_name} AS INT) AS {alias_name}"
    elif target_type == "DECIMAL":
        # Cast to DECIMAL to preserve precision
        return f"CAST({field_name} AS DECIMAL(10, 2)) AS {alias_name}"
    else:
        # Fallback to original casting
        return f"CAST({field_name} AS {target_type}) AS {alias_name}"


def safe_numeric_where_condition(field_name, value, operator="="):
    """
    Creates a safe WHERE condition for numeric fields that might be INT or DOUBLE.

    Args:
        field_name (str): Name of the field
        value (int/float): Value to compare
        operator (str): Comparison operator ('=', '>', '<', etc.)

    Returns:
        str: SQL WHERE condition
    """
    # Cast field to INT for comparison to handle both INT and DOUBLE
    return f"CAST({field_name} AS INT) {operator} {int(value)}"


# Dic de tipos de colunas
COLUMN_TYPE = {
    "cod_idef_pess": STRING,
    "Valor_Contrato_Interno": DECIMAL,
    "Valor_Total_Parcela_Interno": DECIMAL,
    "Gene_Emprego": INTEGER,
    "Vinculo": STRING,
    "Cargo": STRING,
    "Codigo_Exclusao_Comercial": STRING,
    "Cross_Consignado": STRING,
    "Numero_Convenio": STRING,
    "Renda_Sispag": DECIMAL,
    "Target": STRING,
    "Cliente_novo": INTEGER,
    "Claro_Nao": INTEGER,
    "Cod_visao_Cliente": INTEGER,
    "Modalidade_Operacao": INTEGER,
    "Parcela_Mercado": DECIMAL,
    "Apontamento_BX": STRING,
    "Situacao_BX": STRING,
    "Idade": STRING,
    "Indicador_SPO": STRING,
    "Indicador_SPI": INTEGER,
    "Cargos": STRING,
    "CorrentistaAntigo": STRING,
    "Parcela_Elegivel_Refin": STRING,
    "Conta_Pagadora": STRING,
    "Agencia_Pagadora": STRING,
    "tipoInstituicao": STRING,
}


def consulta_tabelas_main(logger, glue_client, spark, glue_context):
    """
    Main function to query tables and return a consolidated dataframe.
    Enhanced with comprehensive error handling and data validation.
    """
    try:
        logger.log_operation_start("Table Query Process")
        
        # Get correntista query with error handling
        try:
            logger.log_operation_start("Correntista Query Generation")
            query_correntista = get_publico_correntista(logger, glue_client)
            
            if not query_correntista or len(query_correntista.strip()) == 0:
                raise DataValidationError("Empty correntista query generated", "query_generation", 
                                        {"query_type": "correntista"})
            
            logger.log_operation_end("Correntista Query Generation")
        except Exception as e:
            raise DataProcessingError("Failed to generate correntista query", "query_generation", 
                                    {"error": str(e)})

        # Execute keyspace queries
        try:
            logger.log_operation_start("Keyspace Data Extraction")
            resultado_df: DataFrame = get_consultas_keyspace(
                glue_client, logger, query_correntista, spark
            )
            
            if resultado_df is None:
                raise DataValidationError("Keyspace query returned null DataFrame", "keyspace_query", 
                                        {"query": query_correntista})
            
            keyspace_count = resultado_df.count()
            logger.log_data_quality("keyspace_extraction", keyspace_count)
            
            if keyspace_count == 0:
                logger.logger.warning("No records found in keyspace tables")
            
        except Exception as e:
            raise DataProcessingError("Failed to execute keyspace queries", "keyspace_extraction", 
                                    {"error": str(e)})

        # Execute Aurora queries
        try:
            logger.log_operation_start("Aurora Data Extraction")
            resultado_df = get_consulta_aurora(
                glue_client, logger, query_correntista, resultado_df, spark
            )
            
            if resultado_df is None:
                raise DataValidationError("Aurora query returned null DataFrame", "aurora_query", 
                                        {"base_records": keyspace_count})
            
            aurora_count = resultado_df.count()
            logger.log_data_quality("aurora_extraction", aurora_count, 
                                   join_ratio=f"{aurora_count/keyspace_count:.2%}" if keyspace_count > 0 else "N/A")
            
        except Exception as e:
            raise DataProcessingError("Failed to execute Aurora queries", "aurora_extraction", 
                                    {"error": str(e), "base_records": keyspace_count})

        # Add missing columns and validate final structure
        try:
            logger.log_operation_start("Schema Standardization")
            resultado_df = add_missing_columns_with_nulls(resultado_df, logger)
            
            # Validate final DataFrame structure
            if resultado_df is None or len(resultado_df.columns) == 0:
                raise DataValidationError("Invalid DataFrame structure after schema standardization", 
                                        "schema_validation", {"columns": 0})
            
            final_count = resultado_df.count()
            final_columns = len(resultado_df.columns)
            
            logger.log_data_quality("schema_standardization", final_count, 
                                   columns=final_columns,
                                   data_retention=f"{final_count/aurora_count:.2%}" if aurora_count > 0 else "N/A")
            
        except Exception as e:
            raise DataProcessingError("Failed to standardize schema", "schema_standardization", 
                                    {"error": str(e)})

        logger.log_operation_end("Table Query Process", 
                               final_records=final_count, 
                               final_columns=final_columns)
        
        return resultado_df
        
    except (DataProcessingError, DataValidationError) as e:
        logger.log_error_with_context(e, e.operation, **e.context)
        raise
    except Exception as e:
        logger.log_error_with_context(e, "table_query_unknown_error")
        raise DataProcessingError("Unexpected error during table queries", "table_query_unknown_error", 
                                {"error": str(e)})


def get_consultas_keyspace(glue_client, logger, query_correntista, spark):
    """
    Enhanced keyspace query function using the new database mapping system.
    Provides better maintainability and standardized table access.
    """
    logger.info("Consultando a tabela tbfc66706_pubi_vsao_pess para obter correntistas")
    resultado_df = consulta_tabelas_mapped(
        resultado_df=None,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        table_key="public_person_view",
        select_fields=["cod_idef_pess"],
        where_conditions=['cod_tipo_cont= "C"', 'cod_cont_ativ= 1']
    )
    logger.info("Validando DF")

    if resultado_df is None:
        logger.error("resultado_df visao pessoa is None")
    else:
        logger.info(f"Consultando a tabela tbmd7023_dtbc_mode_claf_brau")
        resultado_df = consulta_tabelas_mapped(
            resultado_df=resultado_df,
            logger=logger,
            glue_client=glue_client,
            spark=spark,
            table_key="employment_classification",
            select_fields=["cod_chav_coro_clie", "id_gene_emprego"],
            where_conditions=[f"cod_chav_coro_clie IN ({query_correntista})"]
        )

        if resultado_df is None:
            logger.error(f"resultado_df tbmd7023_dtbc_mode_claf_brau is None")
        else:
            logger.info(
                f"resultado_df tbmd7023_dtbc_mode_claf_brau {resultado_df.count()} rows"
            )

        logger.info("Consultando a tabela tbfc6671_crgo_clie_dm")
        resultado_df = consulta_tabelas_mapped(
            resultado_df=resultado_df,
            logger=logger,
            glue_client=glue_client,
            spark=spark,
            table_key="customer_position",
            select_fields=["cod_chav_coro_clie", "nom_vncl", "nom_crgo"],
            where_conditions=[f"cod_chav_coro_clie IN ({query_correntista})"]
        )

        logger.info("Consultando a tabela tbfc6646_risc_cred_csgb")
        resultado_df = consulta_tabelas_mapped(
            resultado_df=resultado_df,
            logger=logger,
            glue_client=glue_client,
            spark=spark,
            table_key="credit_risk_assigned",
            select_fields=["cod_chav_coro_clie", "vlr_totl_cntr", "ind_oper_elgd_rfin", "vlr_totl_parc_oper"],
            where_conditions=[f"cod_chav_coro_clie IN ({query_correntista})", "cod_prod_cred_pre_apra = '027'"]
        )

        logger.info("Consultando a tabela tbfc6553_moti_excu_csgd_dm")
        resultado_df = consulta_tabelas(
            resultado_df=resultado_df,
            logger=logger,
            glue_client=glue_client,
            spark=spark,
            conv=0,
            db="dados_manuais",
            tabela="tbfc6553_moti_excu_csgd_dm",
            select_campos="CAST(cod_chav_coro_clie AS STRING) AS cod_idef_pess,"
            "CAST(cod_prod_cred_pess_fisi AS STRING) AS Codigo_Exclusao_Comercial",
            where_cond=f"cod_chav_coro_clie IN ({query_correntista})",
        )

        logger.info("Consultando a tabela tbfc6431_mode_csgd_crss_dm")
        resultado_df = consulta_tabelas(
            resultado_df=resultado_df,
            logger=logger,
            glue_client=glue_client,
            spark=spark,
            conv=0,
            db="dados_manuais",
            tabela="tbfc6431_mode_csgd_crss_dm",
            select_campos="CAST(cod_chav_coro_clie AS STRING) AS cod_idef_pess, "
            "CAST(ind_rati_csgd AS STRING) AS Cross_Consignado",
            where_cond=f"cod_chav_coro_clie IN  ({query_correntista})",
        )

        logger.info("Consultando a tabela tbfc6432_vncl_orgo_pubi_dm")
        resultado_df = consulta_tabelas(
            resultado_df=resultado_df,
            logger=logger,
            glue_client=glue_client,
            spark=spark,
            conv=0,
            db="dados_manuais",
            tabela="tbfc6432_vncl_orgo_pubi_dm",
            select_campos="CAST(cod_chav_coro_clie AS STRING) AS cod_idef_pess,"
            "CAST(num_ctrt_mae_lgdo AS STRING) AS Numero_Convenio",
            where_cond=f"cod_chav_coro_clie IN ({query_correntista})",
        )

        logger.info("Consultando as tabelas tbfc6682_pgto_salr_spi")
        resultado_df = consulta_tabelas(
            resultado_df=resultado_df,
            logger=logger,
            glue_client=glue_client,
            spark=spark,
            conv=0,
            db="ricdb",
            tabela="tbfc6682_pgto_salr_spi",
            select_campos="CAST(cod_chav_coro_clie AS STRING) AS cod_idef_pess,"
            "CAST(cod_agen_pagd AS INT) AS Indicador_SPI,"
            "CAST(cod_ccor_pagd AS STRING) AS Agencia_Pagadora,"
            "CAST(cod_ccor_pagd AS STRING) AS Conta_Pagadora,"
            "CAST(vlr_lanc_cred_favo AS DECIMAL(10, 2)) AS Renda_Sispag",
            where_cond=f"cod_chav_coro_clie IN ({query_correntista})",
        )

        logger.info("Consultando a tabela tbfc6418_grup_vsao_clie_dm")
        resultado_df = consulta_tabelas(
            resultado_df=resultado_df,
            logger=logger,
            spark=spark,
            conv=0,
            db="dados_manuais",
            tabela="tbfc6418_grup_vsao_clie_dm",
            select_campos="CAST(cod_chav_coro_clie AS STRING) AS cod_idef_pess,"
            "CAST(cod_agru_grup_cred AS STRING) AS Target",
            where_cond=f"cod_chav_coro_clie IN ({query_correntista})",
        )

        if resultado_df is None:
            logger.error("resultado_df is tbfc6418_grup_vsao_clie_dm")
        else:
            logger.info(
                f"resultado_df tbfc6418_grup_vsao_clie_dm has {resultado_df.count()} rows"
            )
            resultado_df = resultado_df.dropDuplicates()
        return resultado_df


def get_consulta_aurora(glue_client, logger, query_correntista, resultado_df, spark):
    """
    Enhanced Aurora query function using the new database mapping system.
    Provides better maintainability and standardized table access.
    """
    # Query customer credit view table using mapped configuration
    logger.info("Consultando a tabela tbfc6034_vsao_cred_clie")
    resultado_df = consulta_tabelas_mapped(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        table_key="customer_credit_view",
        select_fields=["cod_idef_pess", "ind_clie_corn_anti", "ind_inle_frte_clie"],
        where_conditions=[f"cod_idef_pess IN ({query_correntista})"]
    )

    # Query customer model element table using mapped configuration
    logger.info("Consultando a tabela tbfc6230_mode_elto_pf")
    resultado_df = consulta_tabelas_mapped(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        table_key="customer_model_element",
        select_fields=["cod_idef_pess", "cod_mode_elto"],
        where_conditions=[f"cod_idef_pess IN ({query_correntista})"]
    )

    # Query credit risk central bank table using mapped configuration
    logger.info("Consultando a tabela tbfc6177_risc_cred_banc_cenl")
    resultado_df = consulta_tabelas_mapped(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        table_key="credit_risk_central_bank",
        select_fields=["cod_idef_pess", "cod_moda_sbme_oper", "vlr_cred_vncr_30", "cod_inst_finn_cogl"],
        where_conditions=[f"cod_idef_pess IN ({query_correntista})", "cod_tipo_pess = 'F'", f"{safe_numeric_where_condition('cod_inst_finn_cogl', 99)}"]
    )

    # Query additional tables using mapped configuration
    logger.info("Consultando a tabela tbfc6036_apon_cadl_clie")
    resultado_df = consulta_tabelas_mapped(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        table_key="customer_registration_notes",
        select_fields=["cod_idef_pess", "dat_apon_cadl_clie", "cod_tipo_apon_cadl"],
        where_conditions=[f"cod_idef_pess IN ({query_correntista})"]
    )

    logger.info("Consultando a tabela tbfc6037_cred_clie_pfis")
    resultado_df = consulta_tabelas_mapped(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        table_key="customer_credit_individual",
        select_fields=["cod_idef_pess", "dat_nasc_pfis"],
        where_conditions=[f"cod_idef_pess IN ({query_correntista})"]
    )

    logger.info("Consultando a tabela tbfc6505_trsf_cont_salr")
    resultado_df = consulta_tabelas_mapped(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        table_key="salary_account_transfer",
        select_fields=["cod_idef_pess", "cod_mens_tran"],
        where_conditions=[f"cod_idef_pess IN ({query_correntista})"]
    )

    # Final data validation and cleanup
    if resultado_df is None:
        logger.error("resultado_df Aurora")
    else:
        logger.info(f"resultado_df has {resultado_df.count()} rows")
        resultado_df = resultado_df.dropDuplicates()
    
    return resultado_df


def get_consulta_aurora_legacy(glue_client, logger, query_correntista, resultado_df, spark):
    """
    Legacy Aurora query function - maintained for backward compatibility.
    TODO: Remove after full migration to mapped system.
    """
    logger.info("Consultando a tabela tbfc6034_vsao_cred_clie")
    resultado_df = consulta_tabelas(
        resultado_df=resultado_df,
        logger=logger,
        spark=spark,
        conv=0,
        db="ricdb",
        tabela="tbfc6034_vsao_cred_clie",
        select_campos="CAST(cod_idef_pess AS STRING) AS cod_idef_pess,"
        "CAST(Ind_clie_corn_anti AS INT) AS CorrentistaAntigo,"
        "CAST(ind_inle_frte_clie AS INT) AS Claro_Nao",
        where_cond=f"cod_idef_pess IN ({query_correntista})",
    )

    logger.info("Consultando a tabela tbfc6230_mode_elto_pf")
    resultado_df = consulta_tabelas(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        conv=0,
        db="ricdb",
        tabela="tbfc6230_mode_elto_pf",
        select_campos="CAST(cod_idef_pess AS STRING) AS cod_idef_pess,"
        "CAST(cod_mode_elto AS INT) AS Cod_visao_Cliente",
        where_cond=f"cod_idef_pess IN ({query_correntista})",
    )

    logger.info("Consultando a tabela tbfc6177_risc_cred_banc_cenl")
    resultado_df = consulta_tabelas(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        conv=0,
        db="ricdb",
        tabela="tbfc6177_risc_cred_banc_cenl",
        select_campos="CAST(cod_idef_pess AS STRING) AS cod_idef_pess,"
        "CAST(cod_moda_sbme_oper AS INT) AS Modalidade_Operacao,"
        "CAST(vlr_cred_vncr_30 AS DECIMAL(10, 2)) AS Parcela_Mercado,"
        f'{safe_numeric_cast("cod_inst_finn_cogl", "STRING", "tipoInstituicao")}',
        where_cond=f"cod_idef_pess IN ({query_correntista}) AND cod_tipo_pess = 'F' AND {safe_numeric_where_condition('cod_inst_finn_cogl', 99)}",
    )

    logger.info("Consultando a tabela tbfc6036_apon_cadl_clie")
    resultado_df = consulta_tabelas(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        conv=0,
        db="ricdb",
        tabela="tbfc6036_apon_cadl_clie",
        select_campos="CAST(cod_idef_pess AS STRING) AS cod_idef_pess,"
        'DATEDIFF(TO_DATE(CAST(CURRENT_DATE() AS STRING), "yyyy-MM-dd"),'
        'TO_DATE(CAST(dat_apon_cadl_clie AS STRING), "yyyy-MM-dd")) AS Situacao_BX,'
        "CAST(cod_tipo_apon_cadl AS STRING) AS Apontamento_BX",
        where_cond=f"cod_idef_pess IN ({query_correntista})",
    )

    logger.info("Consultando a tabela tbfc6037_cred_clie_pfis")
    resultado_df = consulta_tabelas(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        conv=0,
        db="ricdb",
        tabela="tbfc6037_cred_clie_pfis",
        select_campos="CAST(cod_idef_pess AS STRING) AS cod_idef_pess,"
        "CAST(dat_nasc_pfis AS STRING) AS Idade",
        where_cond=f"cod_idef_pess IN ({query_correntista})",
    )

    logger.info("Consultando a tabela tbfc6505_trsf_cont_salr")
    resultado_df = consulta_tabelas(
        resultado_df=resultado_df,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        conv=0,
        db="ricdb",
        tabela="tbfc6505_trsf_cont_salr",
        select_campos="CAST(cod_idef_pess AS STRING) AS cod_idef_pess,"
        "CAST(cod_mens_tran AS STRING) AS Indicador_SPO",
        where_cond=f"cod_idef_pess IN ({query_correntista})",
    )

    if resultado_df is None:
        logger.error("resultado_df Aurora")
    else:
        logger.info(f"resultado_df has {resultado_df.count()} rows")
        resultado_df = resultado_df.dropDuplicates()
    return resultado_df

def consulta_tabelas_mapped(
    resultado_df: Optional[DataFrame],
    logger,
    glue_client,
    spark,
    table_key: str,
    select_fields: List[str],
    where_conditions: Optional[List[str]] = None,
    join_key: str = "cod_idef_pess",
    join_type: str = "left"
) -> DataFrame:
    """
    Enhanced table query function using database mapping configuration.
    
    Args:
        resultado_df: Existing DataFrame to join with (optional)
        logger: Logger instance
        glue_client: AWS Glue client
        spark: Spark session
        table_key: Key to identify table in mapping configuration
        select_fields: List of SELECT fields with CAST and aliases
        where_conditions: Optional list of WHERE conditions
        join_key: Column to join on (default: cod_idef_pess)
        join_type: Type of join (default: left)
    
    Returns:
        DataFrame with query results
    """
    # Get table mapping
    table_mapping = db_mapping.get_table_mapping(table_key)
    if not table_mapping:
        logger.error(f"Table mapping not found for key: {table_key}")
        return resultado_df
    
    full_table_name = f"{table_mapping.database}.{table_mapping.table}"
    logger.info(f"Querying mapped table: {full_table_name}")
    
    # Build query using mapping configuration
    try:
        full_table_name, select_clause, where_clause = get_mapped_query_parts(
            table_key, select_fields, where_conditions
        )
        
        query = f"SELECT {select_clause} FROM {full_table_name}"
        
        # Add partition filter
        try:
            ultima_particao = get_partition_filter(
                database_name=table_mapping.database, 
                table_name=table_mapping.table, 
                glue_client=glue_client
            )
            if ultima_particao and where_clause:
                query += f" WHERE ({ultima_particao}) AND ({where_clause})"
            elif ultima_particao:
                query += f" WHERE {ultima_particao}"
            elif where_clause:
                query += f" WHERE {where_clause}"
        except glue_client.exceptions.EntityNotFoundException:
            if where_clause:
                query += f" WHERE {where_clause}"
        
        logger.info(f"Executing mapped query: {query}")
        tabela_df = spark.sql(query)
        
        # Check if DataFrame is empty
        if tabela_df.rdd.isEmpty():
            logger.warning(f"Table {full_table_name} returned no data")
            return resultado_df
        
        # Join with existing DataFrame if provided
        if resultado_df is not None:
            resultado_df = resultado_df.join(tabela_df, on=join_key, how=join_type)
        else:
            resultado_df = tabela_df
            
        return resultado_df
        
    except Exception as e:
        logger.error(f"Error querying mapped table {table_key}: {str(e)}")
        return resultado_df


def consulta_tabelas(
    resultado_df,
    logger,
    glue_client,
    spark,
    conv,
    db,
    tabela,
    select_campos,
    where_cond="",
):
    """
    Legacy table query function - maintained for backward compatibility.
    Consider using consulta_tabelas_mapped for new implementations.
    """
    logger.info(f"Buscando dados da tabela: {db}.{tabela}")
    query = f"SELECT {select_campos} FROM {db}.{tabela}"

    try:
        ultima_particao = get_partition_filter(
            database_name=db, table_name=tabela, glue_client=glue_client
        )
    except glue_client.exceptions.EntityNotFoundException:
        logger.warning(f"Tabela {db}.{tabela}: {ultima_particao}")
        if ultima_particao != "" or where_cond != "":
            if ultima_particao != "" and where_cond != "" and conv != 1:
                query += f" WHERE ({ultima_particao}) AND ({where_cond})"
            elif ultima_particao != "" and conv != 1:
                query += f" WHERE {ultima_particao}"
            else:
                query += f" WHERE {where_cond}"

    logger.info(f"Query: {query}")
    tabela_df = spark.sql(query)

    # verifica se o DataFrame resutlaten esta Vazio
    if tabela_df.rdd.isEmpty():
        logger.warning(f"Tabela {db}.{tabela} sem dados")
        return resultado_df

    if resultado_df is not None and conv != 1:
        resultado_df = resultado_df.join(tabela_df, on="cod_idef_pess", how="left")
    elif resultado_df is not None and conv == 1:
        resultado_df = resultado_df.join(tabela_df, on="nrmaer", how="left")
    else:
        resultado_df = tabela_df

    return resultado_df


def consulta_tabelas_convenio(
    resultado_convenio_df,
    logger,
    glue_client,
    spark,
    conv,
    db,
    tabela,
    select_campos,
    where_cond="",
):
    logger.info("SELECT {select_campos} FROM {db}.{tabela}")
    tabela_df = spark.sql(query)

    # verifica se o DataFrame esta Vazio
    if tabela_df.rdd.isEmpty():
        logger.warning(f"Tabela {db}.{tabela} sem dados")
        return resultado_convenio_df

    if resultado_convenio_df is not None and conv != 1:
        resultado_convenio_df = resultado_convenio_df.join(
            tabela_df, on="cod_idef_pess", how="left"
        )
    elif resultado_convenio_df is not None and conv == 1:
        resultado_convenio_df = resultado_convenio_df.join(
            tabela_df, on="nrmaer", how="left"
        )
    else:
        resultado_convenio_df = tabela_df

    return resultado_convenio_df


def spark_sql_query(
    glue_context, spark, query, mapping, transformation_ctx
) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glue_context, transformation_ctx)


def create_data_frame(
    glue_context: GlueContext, database: str, table_name: str
) -> DataFrame:
    columns = []
    try:
        df = glue_context.create_dynamic_frame.from_catalog(
            database, table_name, transformation_ctx="dynamic_frame"
        ).toDF()
        columns = df.dtypes
        return df
    except Exception as e:
        error_data = []
        for column, dtype in columns:
            if dtype == "string":
                error_data.append("-2")
            elif dtype == "boolean":
                error_data.append(False)
            elif dtype == "double":
                error_data.append(-2.0)
            else:
                error_data.append(-2)
        df = glue_context.createDataFrame([error_data], [c[0] for c in columns])
    return df


def add_missing_columns_with_nulls(df, logger=None):
    if logger:
        logger.info("Tratamento colunas vazias")

    if df is None:
        if logger:
            logger.warning("DataFrame is None")
        return None

    for col, col_type in COLUMN_TYPE.items():
        if col not in df.columns:
            if isinstance(col_type, StringType):
                default_value = F.lit("-1")
            elif isinstance(col_type, (IntegerType, DoubleType, DecimalType)):
                default_value = F.lit(0)
            else:
                default_value = F.lit(None)
            df = df.withColumn(col, default_value.cast(col_type))
    return df


def get_publico_correntista(logger, glue_client):
    query_correntista = (
        "SELECT cod_idef_pess FROM "
        "ricdb.tbfc66706_pubi_vsao_pess WHERE cod_tipo_cont='C' AND cod_cont_ativ= 1"
    )
    query_correntista_particao = get_partition_filter(
        table_name="tbfc66706_pubi_vsao_pess",
        database_name="ricdb",
        glue_client=glue_client,
    )
    logger.info(f"correntista_particao: {query_correntista_particao}")
    return query_correntista


def get_convenios(logger, glue_client):
    query_convenios = (
        "SELECT nrmaer FROM "
        "db_corp_servicosdecontratacao_consignado_sor_01.tbazcma WHERE tpempseg=2 "
        "AND cdgrcons != 'INSS'"
    )
    query_convenios_particao = get_partition_filter(
        table_name="tbazcma",
        database_name="db_corp_servicosdecontratacao_consignado_sor_01",
        glue_client=glue_client,
    )
    return query_convenios


def get_agded(logger, glue_client):
    query_agded = (
        "SELECT cdagecdt, cdctacdt FROM "
        "db_corp_servicosdecontratacao_consignado_sor_01.tbazcma WHERE tpempseg=2 "
        "AND cdgrcons != 'INSS'"
    )
    return query_agded
