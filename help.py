import sys
import time
import boto3
from botocore.exceptions import ClientError

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from utils.escrita_dados import escrita_dados
from utils.leitura_dados import consulta_tabelas_main
from utils.tratamento_dados import formata_df
from utils.logging_config import get_etl_logger, ETLException, DataProcessingError, AWSServiceError
from utils.refresh_boto_session import RefreshableBotoSession

# Initialize logger
logger = get_etl_logger("dataprep_consignado_op")


def main():
    """Main ETL function with enhanced error handling and logging."""
    start_time = time.time()
    job_context = None
    
    try:
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
                "S3_BUCKET_NAME",
                "S3_BUCKET_PREFIX",
            ],
        )

        logger.log_operation_start("ETL Job", job_name=args["JOB_NAME"])

        # Initialize Spark and Glue contexts
        try:
            sc = SparkContext.getOrCreate()
            glue_context = GlueContext(sc)
            spark = glue_context.spark_session
            job_context = Job(glue_context)
            job_context.init(args["JOB_NAME"], args)
            logger.log_operation_end("Spark/Glue Context Initialization")
        except Exception as e:
            raise ETLException("Failed to initialize Spark/Glue context", "initialization", {"error": str(e)})

        # Setup AWS clients with RefreshableBotoSession for better session management
        try:
            logger.log_operation_start("AWS Client Configuration")
            
            # Create refreshable session for Glue operations with role assumption
            refreshable_session_manager = RefreshableBotoSession(
                region_name="sa-east-1",
                sts_arn=args["ROLE_ARN_DATAMESH"],
                session_name="dataprep-consignado-session",
                session_ttl=3600  # 1 hour session
            )
            
            # Get the refreshable session
            aws_session = refreshable_session_manager.refreshable_session()
            
            # Create Glue client with refreshable session
            glue_client = aws_session.client('glue')
            
            # Create S3 client with refreshable session
            s3_client = aws_session.client("s3")
            
            logger.log_aws_operation("RefreshableBotoSession", "Client Setup", "SUCCESS")
        except (ClientError, Exception) as e:
            # Only catch AWS-specific errors and convert to AWSServiceError
            # Let other unexpected exceptions bubble up to the general handler
            if isinstance(e, ClientError) or "AWS" in str(e) or "boto" in str(e).lower():
                raise AWSServiceError("Failed to configure AWS clients", "aws_setup", {"role_arn": args.get("ROLE_ARN_DATAMESH")})
            else:
                # Re-raise unexpected exceptions to be handled by the general exception handler
                raise

        # Extract and validate parameters
        try:
            database_name_spec = args["DATABASE_SPEC"]
            table_name_spec = args["TABLE_SPEC"]
            lake_control_account_id = args["LAKE_CONTROL_ACCOUNT_ID"]
            numero_lote_payload_dmp = int(args["NUMERO_LOTE_PAYLOAD_DMP"])
            numero_lote_spec = int(args["NUMERO_LOTE_SPEC"])
            s3_bucket_name = args["S3_BUCKET_NAME"]
            s3_bucket_prefix = args["S3_BUCKET_PREFIX"]

            # Validate parameters
            if numero_lote_payload_dmp <= 0 or numero_lote_spec <= 0:
                raise ValueError("Batch sizes must be positive integers")
            
            if not s3_bucket_name or not s3_bucket_prefix:
                raise ValueError("S3 bucket name and prefix must be provided")

            logger.log_operation_start("Parameter Validation", 
                                     database=database_name_spec,
                                     table=table_name_spec,
                                     payload_batch_size=numero_lote_payload_dmp,
                                     spec_batch_size=numero_lote_spec)
        except (ValueError, KeyError) as e:
            raise ETLException("Invalid job parameters", "parameter_validation", {"error": str(e)})

        # Data extraction phase
        try:
            logger.log_operation_start("Data Extraction")
            resultado_df = consulta_tabelas_main(logger, glue_client, spark, glue_context)
            record_count = resultado_df.count()
            
            if record_count == 0:
                logger.logger.warning("No records found in source tables")
            
            logger.log_data_quality("extraction", record_count)
        except Exception as e:
            raise DataProcessingError("Failed to extract data from tables", "data_extraction", {"error": str(e)})

        # Data transformation phase
        try:
            logger.log_operation_start("Data Transformation")
            resultado_df = formata_df(resultado_df, numero_lote_payload_dmp, logger)
            transformed_count = resultado_df.count()
            logger.log_data_quality("transformation", transformed_count, 
                                   transformation_ratio=f"{transformed_count/record_count:.2%}" if record_count > 0 else "N/A")
        except Exception as e:
            raise DataProcessingError("Failed to transform data", "data_transformation", {"error": str(e)})

        # Data loading phase
        try:
            logger.log_operation_start("Data Loading")
            escrita_dados(
                resultado_df,
                numero_lote_spec,
                logger,
                glue_client,
                database_name_spec,
                table_name_spec,
                lake_control_account_id,
                s3_client,
                s3_bucket_name,
                s3_bucket_prefix,
            )
            logger.log_aws_operation("S3", "Data Write", "SUCCESS", 
                                   table=table_name_spec, 
                                   records=transformed_count)
        except Exception as e:
            raise DataProcessingError("Failed to write data to S3", "data_loading", {"error": str(e)})

        # Job completion
        total_duration = time.time() - start_time
        logger.log_operation_end("ETL Job", total_duration)
        
        if job_context:
            job_context.commit()

    except ETLException as e:
        # Filter out 'error' key to avoid parameter conflict
        context = {k: v for k, v in e.context.items() if k != 'error'}
        logger.log_error_with_context(e, e.operation, **context)
        if job_context:
            job_context.commit()  # Commit even on failure for AWS Glue tracking
        raise
    except Exception as e:
        logger.log_error_with_context(e, "unknown_error")
        if job_context:
            job_context.commit()
        raise ETLException("Unexpected error during ETL execution", "unknown_error", {"error": str(e)})


if __name__ == "__main__":
    main()
