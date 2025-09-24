"""
Funções de consulta de banco de dados para preparação de dados
Baseado nas especificações do atividade.md
"""

from logging import Logger
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from .data_validation import log_dataframe_info, validate_dataframe_columns
from .database_mapping import DatabaseMappingConfig, build_standardized_query


def consulta_tabelas_main(
    spark: SparkSession,
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Função principal para consultar todas as tabelas e combinar resultados
    """
    logger.info("Iniciando consultas principais das tabelas")
    
    # Obtém consultas do keyspace
    df_keyspace = get_consultas_keyspace(spark, data_referencia, logger)
    
    # Obtém consultas do Aurora
    df_aurora = get_consulta_aurora(spark, data_referencia, logger)
    
    # Combina resultados
    resultado_df = df_keyspace.union(df_aurora)
    
    logger.info(f"Contagem do resultado da consulta combinada: {resultado_df.count()}")
    return resultado_df


def get_consultas_keyspace(
    spark: SparkSession,
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Executa consultas para tabelas do Keyspace
    """
    logger.info("Executando consultas do Keyspace")
    
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
                spark, table_name, data_referencia, logger
            )
            if df is not None:
                dataframes.append(df)
        except Exception as e:
            logger.error(f"Erro ao consultar tabela {table_name}: {str(e)}")
            continue
    
    if dataframes:
        resultado_df = dataframes[0]
        for df in dataframes[1:]:
            resultado_df = resultado_df.union(df)
        return resultado_df
    else:
        # Retorna DataFrame vazio com esquema esperado
        return spark.createDataFrame([], schema="cod_idef_pess string")


def get_consulta_aurora(
    spark: SparkSession,
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Executa consultas para tabelas do Aurora
    """
    logger.info("Executando consultas do Aurora")
    
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
                spark, table_name, data_referencia, logger
            )
            if df is not None:
                dataframes.append(df)
        except Exception as e:
            logger.error(f"Erro ao consultar tabela do Aurora {table_name}: {str(e)}")
            continue
    
    if dataframes:
        resultado_df = dataframes[0]
        for df in dataframes[1:]:
            resultado_df = resultado_df.union(df)
        return resultado_df
    else:
        # Retorna DataFrame vazio com esquema esperado
        return spark.createDataFrame([], schema="cod_idef_pess string")


def consulta_tabelas_mapped(
    spark: SparkSession,
    table_name: str,
    data_referencia: str,
    logger: Logger
) -> Optional[DataFrame]:
    """
    Consulta tabelas usando configuração de mapeamento de banco de dados
    """
    try:
        db_config = DatabaseMappingConfig()
        
        if table_name not in db_config.table_mappings:
            logger.warning(f"Tabela {table_name} não encontrada nos mapeamentos")
            return None
        
        # Prepara condições WHERE apenas para data de referência
        where_conditions = []
        
        # Adiciona filtro de data de referência se necessário
        if data_referencia:
            date_condition = f"data_referencia = '{data_referencia}'"
            where_conditions.append(date_condition)
        
        # Constrói consulta padronizada com parâmetros corretos
        query = build_standardized_query(
            table_key=table_name,
            select_fields=["*"],  # Seleciona todos os campos
            where_conditions=where_conditions
        )
        
        logger.info(f"Executando consulta para tabela {table_name}")
        logger.debug(f"Consulta: {query}")
        
        # Executa consulta
        df = spark.sql(query)
        
        # Valida e registra informações do DataFrame
        validate_dataframe_columns(df, logger)
        log_dataframe_info(df, table_name, logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Erro em consulta_tabelas_mapped para {table_name}: {str(e)}")
        return None


def consulta_tabelas_convenio(
    spark: SparkSession,
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Consulta tabelas de convênio com lógica específica
    """
    logger.info("Executando consultas de convênio")
    
    try:
        # Constrói consulta de convênio
        query = f"""
        SELECT 
            cod_idef_pess,
            numero_convenio,
            nome_convenio,
            data_inicio_convenio,
            data_fim_convenio
        FROM events_convenio
        WHERE data_referencia = '{data_referencia}'
        """
        
        df = spark.sql(query)
        log_dataframe_info(df, "events_convenio", logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Erro em consulta_tabelas_convenio: {str(e)}")
        return spark.createDataFrame([], schema="cod_idef_pess string")


def spark_sql_query(
    spark: SparkSession,
    query: str,
    logger: Logger
) -> Optional[DataFrame]:
    """
    Executa consulta SQL com tratamento de erro
    """
    try:
        logger.debug(f"Executando consulta SQL: {query}")
        df = spark.sql(query)
        return df
    except Exception as e:
        logger.error(f"Erro ao executar consulta SQL: {str(e)}")
        return None


def create_data_frame(
    spark: SparkSession,
    database: str,
    table: str,
    logger: Logger
) -> Optional[DataFrame]:
    """
    Cria DataFrame a partir do catálogo do Glue
    """
    try:
        full_table_name = f"{database}.{table}"
        logger.info(f"Criando DataFrame para {full_table_name}")
        
        df = spark.table(full_table_name)
        return df
        
    except Exception as e:
        logger.error(f"Erro ao criar DataFrame para {database}.{table}: {str(e)}")
        return None


def add_missing_columns_with_nulls(
    df: DataFrame,
    expected_columns: List[str],
    logger: Logger
) -> DataFrame:
    """
    Adiciona colunas ausentes com valores nulos
    """
    try:
        existing_columns = df.columns
        missing_columns = [col for col in expected_columns if col not in existing_columns]
        
        if missing_columns:
            logger.info(f"Adicionando colunas ausentes: {missing_columns}")
            for col_name in missing_columns:
                df = df.withColumn(col_name, lit(None))
        
        return df.select(*expected_columns)
        
    except Exception as e:
        logger.error(f"Erro ao adicionar colunas ausentes: {str(e)}")
        return df


def get_publico_correntista(
    spark: SparkSession,
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Obtém dados de correntista público
    """
    logger.info("Obtendo dados de correntista público")
    
    try:
        query = f"""
        SELECT 
            cod_idef_pess,
            indicador_publico,
            data_inicio_conta,
            data_ultima_movimentacao
        FROM public_person_view
        WHERE data_referencia = '{data_referencia}'
        """
        
        df = spark.sql(query)
        log_dataframe_info(df, "public_person_view", logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Erro em get_publico_correntista: {str(e)}")
        return spark.createDataFrame([], schema="cod_idef_pess string")


def get_convenios(
    spark: SparkSession,
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Obtém dados de convênio com lógica de negócio específica
    """
    logger.info("Obtendo dados de convênio")
    
    try:
        query = f"""
        SELECT 
            cod_idef_pess,
            numero_convenio,
            tipo_convenio,
            margem_disponivel,
            valor_parcela_atual
        FROM marg_cred
        WHERE data_referencia = '{data_referencia}'
        AND status_convenio = 'ATIVO'
        """
        
        df = spark.sql(query)
        log_dataframe_info(df, "marg_cred", logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Erro em get_convenios: {str(e)}")
        return spark.createDataFrame([], schema="cod_idef_pess string")


def get_agded(
    spark: SparkSession,
    data_referencia: str,
    logger: Logger
) -> DataFrame:
    """
    Obtém dados AGDED com transformações específicas
    """
    logger.info("Obtendo dados AGDED")
    
    try:
        query = f"""
        SELECT 
            cod_idef_pess,
            codigo_agencia,
            digito_agencia,
            nome_agencia,
            codigo_produto
        FROM customer_registration_notes
        WHERE data_referencia = '{data_referencia}'
        """
        
        df = spark.sql(query)
        log_dataframe_info(df, "customer_registration_notes", logger)
        
        return df
        
    except Exception as e:
        logger.error(f"Erro em get_agded: {str(e)}")
        return spark.createDataFrame([], schema="cod_idef_pess string")
