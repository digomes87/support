import logging
import sys
from math import log
from string import Formatter

import boto3
from awsglue.context import GlueContext
from awsglue.job import job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from utils.escrita_dados import escrita_dados
from utils.leitura_dados import consulta_tabelas
from utils.refresh_boto_session import RefreshableBotoSession
from utils.tratamento_dados import formata_df

MSG_FORMAT = "%(asctime)s %(levelnamem)s %(name)s: %(message)s%"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


def main():
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
        ],
    )

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger = get_logger()
    logger.info("Iniciando execução do Job...")

    try:
        # session boto3
        logger.info("Configurancao sessoes do boto3")
        refresh_boto_session = RefreshableBotoSession().refreshable_session()
        refresh_boto_session_role = RefreshableBotoSession(
            sts_arn=args["ROLE_ARN_DATAMESH"], session_name="addPartition"
        ).refreshable_session()
        glue_client = refresh_boto_session_role.client("glue")
        s3_client = refresh_boto_session.client("s3")

        # param
        database_name_spec = args["DATABASE_SPEC"]
        table_name_spec = args["TABLE_SPEC"]
        lake_control_account_id = args["LAKE_CONTROL_ACCOUNT_ID"]
        numero_lote_payload_dmp = int(args["NUMERO_LOTE_PAYLOAD_DMP"])
        numero_lote_spec = int(args["NUMERO_LOTE_SPEC"])

        logger.info("Paramentros recebidos")
        logger.info(f"DATABASE_SPEC: {database_name_spec}")
        logger.info(f"TABLE_SPEC: {table_name_spec}")
        logger.info(f"LAKE_CONTROL_ACCOUNT_ID: {lake_control_account_id}")
        logger.info(f"NUMERO_LOTE_PAYLOAD_DMP: {numero_lote_payload_dmp}")
        logger.info(f"NUMERO_LOTE_SPEC: {numero_lote_spec}")

        # consulta consulta tbelas
        logger.info("Consultando tabelas ....")
        resultado_df = consulta_tabelas(logger, glue_client, spark, glue_context)
        logger.info(
            f"Resultado da consulta {resultado_df.count()} registros encontrados"
        )

        # formatacao do Dataframe
        logger.info("Formantando Dataframe")
        resultado_df = formata_df(resultado_df, numero_lote_payload_dmp, logger)
        logger.info(f"DataFrame formatado com sucesso: {resultado_df}")

        # Escrita dos dados
        logger.info("Escrevendo dados ....")
        escrita_dados(
            resultado_df,
            numero_lote_spec,
            logger,
            glue_client,
            database_name_spec,
            table_name_spec,
            lake_control_account_id,
            s3_client,
        )
        logger.info("Dados escritos com sucesso")

    except Exception as e:
        logger.error("Erro durante a execução do Job.", exc_info=True)
        raise e

    logger.info("Fim da execucao do processo")
    job.commit()


def get_logger():
    logger = logging.getLogger("aumento_ativo_cartoes")
    logger.setLevel(logger.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


if __name__ == "__main__":
    main()
