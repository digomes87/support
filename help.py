import sys
import logging
from logging import Logger

import boto3
from pyspark import SparkContext
from pyspark.sql import SparkSession

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from utils.escrita_dados import write_to_s3
from utils.leitura_dados import leitura_dados
from utils.refresh_boto_session import RefreshableBotoSession
from utils.tratamento_dados import (
    compacta_em_lotes, formata_payload_dmp, tratamento_df,
    agrupa_por_id_pessoa, agregacoes_campo, organiza_estruturas_pos_agrupamento,
    set_IndicadorSPI, set_IndicadorSPO, set_indicadorCNAO, calcula_idade
)
from utils.database_queries import consulta_tabelas_main
from utils.data_validation import validate_etl_output_format

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
    """Main ETL function for consignado OP data preparation."""
    
    # Initialize logger
    logger = get_logger()
    
    # Parse command line arguments
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
            "LISTA_CPF",
            "DATA_REFERENCIA",
        ],
    )

    logger.info(f"Starting ETL Job: {args['JOB_NAME']}")

    # Initialize Spark and Glue contexts
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Setup AWS clients with RefreshableBotoSession
    refresh_boto_session = RefreshableBotoSession().refreshable_session()
    refresh_boto_session_role = RefreshableBotoSession(
        sts_arn=args["ROLE_ARN_DATAMESH"], session_name="addPartition"
    ).refreshable_session()

    glue_client_role = refresh_boto_session_role.client("glue")
    s3_client = refresh_boto_session.client("s3")

    # Extract parameters
    database_name_spec = args["DATABASE_SPEC"]
    table_name_spec = args["TABLE_SPEC"]
    lake_control_account_id = args["LAKE_CONTROL_ACCOUNT_ID"]
    numero_lote_payload_dmp = int(args["NUMERO_LOTE_PAYLOAD_DMP"])
    numero_lote_spec = int(args["NUMERO_LOTE_SPEC"])
    lista_cpf = args["LISTA_CPF"].split(",") if args.get("LISTA_CPF") else []
    data_referencia = args.get("DATA_REFERENCIA", "")

    logger.info(f"Processing data for database: {database_name_spec}, table: {table_name_spec}")
    logger.info(f"Processing {len(lista_cpf)} CPFs for reference date: {data_referencia}")

    logger.info("Starting ETL Job execution...")

    # Step 1: Query all tables and get combined results
    logger.info("Step 1: Querying database tables")
    df_resultado = consulta_tabelas_main(spark, lista_cpf, data_referencia, logger)
    
    if df_resultado.count() == 0:
        logger.warning("No data found in database queries")
        return

    # Step 2: Apply data treatments and transformations
    logger.info("Step 2: Applying data treatments")
    df_resultado = tratamento_df(df_resultado, logger)
    df_resultado = calcula_idade(df_resultado)
    df_resultado = set_IndicadorSPI(df_resultado)
    df_resultado = set_IndicadorSPO(df_resultado)
    df_resultado = set_indicadorCNAO(df_resultado)

    # Step 3: Group by person ID and apply window functions
    logger.info("Step 3: Grouping data by person ID")
    df_resultado = agrupa_por_id_pessoa(df_resultado)

    # Step 4: Apply field aggregations
    logger.info("Step 4: Applying field aggregations")
    df_resultado = agregacoes_campo(df_resultado)

    # Step 5: Organize structures after grouping
    logger.info("Step 5: Organizing final JSON structure")
    df_resultado = organiza_estruturas_pos_agrupamento(df_resultado)

    # Step 6: Format payload for DMP
    logger.info("Step 6: Formatting payload for DMP")
    df_consig_op_batch = formata_payload_dmp(df_resultado, numero_lote_payload_dmp, logger)

    # Step 7: Compact into batches
    logger.info("Step 7: Compacting data into batches")
    df_consig_op_batch = compacta_em_lotes(df_consig_op_batch, numero_lote_payload_dmp)

    # Step 8: Validate output format
    logger.info("Step 8: Validating output format")
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
