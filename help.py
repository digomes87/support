import logging
from logging import Logger

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()

MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIMEZONE_SP = "America/Sao_Paulo"

logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def spark_sql_query(glue_context, spark, query, mapping, transformation_ctx) -> DynamicFrame:
    """Executa consulta SQL em DataFrames do Spark."""
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTemView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glue_context, transformation_ctx)


def create_data_frame(glue_context: GlueContext, database: str, table_name: str) -> DataFrame:
    """Cria DataFrame a partir de tabela do catálogo do Glue."""
    columns = []
    try:
        df = glue_context.create_dynamic_frame.from_catalog(
                database, table_name, transformation_ctx="dynamic_frame"
            ).toDF()
        columns = df.dtypes
        return df
    except Exception as e:
        logger.error(f"Erro ao criar DataFrame para tabela {table_name}: {str(e)}")
        # Cria DataFrame vazio com dados de erro
        error_data = []
        for column, dtype in columns:
            if dtype == "string":
                error_data.append("-2")
            elif dtype == "boolean":
                error_data.append(False)
            elif dtype == "double":
                error_data.append(-2.0)
            else:
                error_data.append(None)
        
        if error_data:
            return spark.createDataFrame([error_data], [col[0] for col in columns])
        else:
            return spark.createDataFrame([], "cod_idef_pess string")


def leitura_dados(logger: Logger, spark: SparkSession, glue_context: GlueContext, data_referencia: str) -> DataFrame:
    """Função principal para ler dados de tabelas do catálogo usando configurações adequadas de banco de dados."""
    
    logger.info("Iniciando extração de dados das tabelas do catálogo")
    
    try:
        # Importa função de consultas de banco de dados
        from .database_queries import consulta_tabelas_main

        # Usa a função adequada de consulta de banco de dados que integra com database_mapping
        logger.info("Executando consultas integradas de banco de dados...")
        result_df = consulta_tabelas_main(
            spark=spark,
            data_referencia=data_referencia,
            logger=logger
        )
        
        # Conta registros
        rows = result_df.count()
        logger.info(f"Total de registros extraídos: {rows}")
        logger.info("Extração de dados finalizada")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Erro na extração de dados: {str(e)}")
        # Retorna DataFrame vazio em caso de erro
        return spark.createDataFrame([], "cod_idef_pess string")
