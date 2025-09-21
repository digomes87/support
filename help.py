import logging
from math import log
from typing import Mapping

import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame, SparkSession, column
from pyspark.sql.catalog import Database
from pyspark.sql.functions.builtin import conv
from pyspark.sql.group import df_varargs_api
from pyspark.sql.streaming import query
from pyspark.sql.types import (DecimalType, DoubleType, IntegerType,
                               StringType, TimestampType)
from utils.get_partition_filter import get_partition_filter

spark = SparkSession.builder.getOrCreate()

MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
DATETIME_FORMAT = "%Y-%M-%d %H:%M:%S"
TIMEZONE_SP = "America/Sao_Paulo"


logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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


def consulta_tabelas(logger, glue_client, spark, glue_context):
    """Consutal tabelas e retorna um dataframe"""
    query_correntista = get_publico_correntista(logger, glue_client)

    resultado_df: DataFrame = get_consultas_keyspace(
        glue_client, logger, query_correntista, spark
    )
    resultado_df = get_consulta_aurora(
        glue_client, logger, query_correntista, resultado_df, spark
    )
    resultado_df = add_missing_columns_with_nulls(resultado_df)
    convenio_join: DataFrame = get_consultas_mesh(logger, spark, glue_context)
    df_cruzado = cruzar_dataframes(resultado_df, convenio_join)

    return df_cruzado


def get_consultas_keyspace(glue_client, logger, query_correntista, spark):
    logger.info("Consultando a tabela tbfc66706_pubi_vsao_pess para obter correntistas")
    resultado_df = consulta_tabelas(
        resultado_df=None,
        logger=logger,
        glue_client=glue_client,
        spark=spark,
        conv=0,
        db="ricdb",
        tabela="tbfc66706_pubi_vsao_pess",
        select_campos="CAST(cod_idef_pess as STRING) AS cod_idef_pess",
        where_cond='cod_tipo_cont= "C" and cod_cont_ativ= 1 ',
    )
    logger.info("Validando DF")

    if resultado_df is None:
        logger.error("resultado_df visao pessoa is None")
    else:
        logger.info(f"Consultando a tabela tbmd7023_dtbc_mode_claf_brau")
        resultado_df = consulta_tabelas(
            resultado_df=resultado_df,
            logger=logger,
            glue_client=glue_client,
            spark=spark,
            conv=0,
            db="ricdb",
            tabela="tbmd7023_dtbc_mode_claf_brau",
            select_campos="CAST(cod_chav_coro_clie as STRING) AS cod_idef_pess,"
            "CAST(id_gene_emprego AS INT) AS Gene_Emprego",
            where_cond=f"cod_chav_coro_clie IN ({query_correntista})",
        )

        if resultado_df is None:
            logger.error(f"resultado_df tbmd7023_dtbc_mode_claf_brau is None")
        else:
            logger.info(
                f"resultado_df tbmd7023_dtbc_mode_claf_brau {resultado_df.count()} rows"
            )

        logger.info("Consultando a tabela tbfc6671_crgo_clie_dm")
        resultado_df = consulta_tabelas(
            resultado_df=resultado_df,
            logger=logger,
            glue_client=glue_client,
            spark=spark,
            conv=0,
            db="dados_manuais",
            tabela="tbfc6671_crgo_clie_dm",
            select_campos="CAST(cod_chav_coro_clie AS STRING) AS cod_idef_pess,"
            "CAST(nom_vncl AS STRING) AS Vinculo,"
            "CAST(nom_crgo AS STRING) AS Cargo",
            where_cond=f"cod_chav_coro_clie IN ({query_correntista})",
        )

        logger.info("Consultando a tabela tbfc6646_risc_cred_csgb")
        resultado_df = resultado_df(
            resultado_df=resultado_df,
            logger=logger,
            glue_client=glue_client,
            spark=spark,
            conv=0,
            db="ricdb",
            tabela="tbfc6646_risc_cred_csgb",
            select_campos="CAST(cod_chav_coro_clie AS STRING) AS cod_idef_pess,"
            "CAST(vlr_totl_cntr AS DECIMAL(10, 2)) AS Valor_Contrato_Interno,"
            "CAST(ind_oper_elgd_rfin AS DECIMAL(10, 2)) AS Parcela_Elegivel_Refin,"
            "CAST(vlr_totl_parc_oper AS DECIMAL(10, 2)) AS Valor_Total_Parcela_Interno",
            where_cond=f"cod_chav_coro_clie IN ({query_correntista}) and cod_prod_cred_pre_apra = '027' ",
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
        select_campos="CAST(cod_idef_pess AS STRING) AS cod_idef_pess,"
        'DATEDIFF(TO_DATE(CAST(CURRENT_DATE() AS STRING), "yyyy-MM-dd"),'
        'TO_DATE(CAST(dat_apon_cadl_clie AS STRING), "yyyy-MM-dd")) AS Situacao_BX,'
        "CAST(cod_tipo_apon_cadl AS STRING) AS Apontamento_BX",
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


def get_consultas_mesh(logger, spark, glue_client):
    logger.info("Inicio da consulta: TBAZCMA")
    visao_convenio_AZ = create_data_frame(
        glue_client,
        database="db_corp_servicosdecontratacao_consignado_sor_01",
        table_name="TBAZCMA",
    )
    visao_convenio_AZ = visao_convenio_AZ.select(
        "nrmaer", "cdagecdt", "cdagecdt", "tpempseg", "qtpzmax", "cdgrcons"
    )
    logger.info("Fim da consulta: TBAZCMA")

    logger.info("Inicio da consulta: TBAZCCV")
    visao_convenio_AV = create_data_frame(
        glue_context,
        database="db_corp_servicosdecontratacao_consignado_sor_01",
        table_name="tbazccv",
    )
    visao_convenio_AV = visao_convenio_AV.select("nrmaer", "pct_maxi_cpmm_rend")
    logger.info("Fim da consulta: tbazccv")

    logger.info("Realiando o join entre TBAZCMA e tbazccv usando o campo nrmaer")

    # Realiando o join entre os dataframe
    convenio_join = visao_convenio_AZ.join(visao_convenio_AV, on="nrmaer", how="left")
    convenio_join = convenio_join.dropDuplicates(["nrmaer"])

    return convenio_join


def cruzar_dataframes(resultado_df: DataFrame, convenio_join: DataFrame) -> DataFrame:
    # Join entre os dataframe com base nas condicoes
    df_cruzado = resultado_df.join(
        convenio_join,
        (resultado_df["Agencia_Pagadora"] == convenio_join["cdagecdt"])
        & (resultado_df["Conta_Pagadora"])
        == convenio_join["cdctacdt"],
        how="inner",
    )
    return df_cruzado


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
                query += f"WHERE ({ultima_particao}) AND ({where_cond})"
            elif ultima_particao != "" and conv != 1:
                query += f"WHERE {ultima_particao}"
            else:
                query += f"WHERE {where_cond}"

    logger.info("Query: {query}")
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


def add_missing_columns_with_nulls(df):
    logger.info("Tratamento colunas vazias")

    if df is None:
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
