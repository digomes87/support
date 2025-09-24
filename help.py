import logging
import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import SparkSession
from utils.data_validation import validate_etl_output_format
from utils.escrita_dados import write_to_s3
from utils.leitura_dados import leitura_dados
from utils.refresh_boto_session import RefreshableBotoSession
from utils.tratamento_dados import (agregacoes_campo, agrupa_por_id_pessoa,
                                    calcula_idade, compacta_em_lotes,
                                    formata_payload_dmp,
                                    organiza_estruturas_pos_agrupamento,
                                    set_indicadorCNAO, set_IndicadorSPI,
                                    set_IndicadorSPO, tratamento_df)


def get_logger():
    logger = logging.getLogger("pre_aprovado_consig_op")
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s: %(message)s', '%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

spark = SparkSession.builder.getOrCreate()


def main():
    """Função principal de ETL para preparação de dados de consignado OP."""
    
    # Inicializa logger
    logger = get_logger()
    
    # Analisa argumentos da linha de comando
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DATABASE_SPEC",
            "TABLE_SPEC",
            "LAKE_CONTROL_ACCOUNT_ID",
            "ROLE_ARN_DATAMESH",
            "NUMERO_LOTE_PAYLOAD_DMP",
            "NUMERO_LOTE_SPEC",
            "S3_BUCKET_NAME",
            "S3_BUCKET_PREFIX",
        ],
    )

    logger.info(f"Iniciando Job ETL: {args['JOB_NAME']}")

    # Inicializa contextos Spark e Glue
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Configura clientes AWS com RefreshableBotoSession
    refresh_boto_session = RefreshableBotoSession().refreshable_session()
    refresh_boto_session_role = RefreshableBotoSession(
        sts_arn=args["ROLE_ARN_DATAMESH"], session_name="addPartition"
    ).refreshable_session()

    glue_client_role = refresh_boto_session_role.client("glue")
    s3_client = refresh_boto_session.client("s3")

    # Extrai parâmetros
    database_name_spec = args["DATABASE_SPEC"]
    table_name_spec = args["TABLE_SPEC"]
    lake_control_account_id = args["LAKE_CONTROL_ACCOUNT_ID"]
    numero_lote_payload_dmp = int(args["NUMERO_LOTE_PAYLOAD_DMP"])
    numero_lote_spec = int(args["NUMERO_LOTE_SPEC"])
    
    # Generate current date automatically for system execution
    data_referencia = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Using current execution date: {data_referencia}")

    logger.info(f"Processando dados para banco: {database_name_spec}, tabela: {table_name_spec}")
    logger.info(f"Data de referência para execução: {data_referencia}")

    logger.info("Iniciando execução do Job ETL...")

    # Passo 1: Consulta todas as tabelas e obtém resultados combinados
    logger.info("Passo 1: Lendo dados das tabelas do catálogo")
    df_resultado = leitura_dados(logger, spark, glue_context, data_referencia)
    
    if df_resultado.count() == 0:
        logger.warning("Nenhum dado encontrado nas consultas do banco de dados")
        return

    # Passo 2: Aplica tratamentos e transformações de dados
    logger.info("Passo 2: Aplicando tratamentos de dados")
    df_resultado = tratamento_df(df_resultado, logger)
    df_resultado = calcula_idade(df_resultado)
    df_resultado = set_IndicadorSPI(df_resultado)
    df_resultado = set_IndicadorSPO(df_resultado)
    df_resultado = set_indicadorCNAO(df_resultado)

    # Passo 3: Agrupa por ID da pessoa e aplica funções de janela
    logger.info("Passo 3: Agrupando dados por ID da pessoa")
    df_resultado = agrupa_por_id_pessoa(df_resultado)

    # Passo 4: Aplica agregações de campo
    logger.info("Passo 4: Aplicando agregações de campo")
    df_resultado = agregacoes_campo(df_resultado)

    # Passo 5: Organiza estruturas após agrupamento
    logger.info("Passo 5: Organizando estrutura JSON final")
    df_resultado = organiza_estruturas_pos_agrupamento(df_resultado)

    # Passo 6: Formata payload para DMP
    logger.info("Passo 6: Formatando payload para DMP")
    df_consig_op_batch = formata_payload_dmp(df_resultado, numero_lote_payload_dmp, logger)

    # Passo 7: Compacta em lotes
    logger.info("Passo 7: Compactando dados em lotes")
    df_consig_op_batch = compacta_em_lotes(df_consig_op_batch, numero_lote_payload_dmp)

    # Passo 8: Valida formato de saída
    logger.info("Passo 8: Validando formato de saída")
    validate_etl_output_format(df_consig_op_batch, logger)

    write_to_s3(
        df_consig_op_batch,
        glue_client_role,
        database_name_spec,
        table_name_spec,
        lake_control_account_id,
        numero_lote_spec,
        logger,
        s3_client,
    )

    logger.info("Fim de execucao do processo")
    job.commit()


if __name__ == "__main__":
    main()
