#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AWS Glue ETL Job - TBFC6 Consig INSS Checklist to PA Consolidado

Author: Senior Software Engineer
Version: 1.0.0
Description: Processo ETL automatizado para extrair dados da tabela tbfc6_consig_inss_checklist,
             processar o campo payloadoutputdmps e carregar na tabela tbfc6_pa_consolidado_inss.

Requirements:
    - AWS Glue 4.0
    - PySpark 3.3+
    - Python 3.9+
"""

import logging
import sys
import uuid
import json
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
from enum import Enum

import boto3
from pydantic import BaseModel, Field, field_validator, ValidationInfo
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructField, StructType, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType
)
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import SparkConf


# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

class Constants:
    """Application constants."""
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY_MS = 1000
    
    # File formats
    FORMAT_PARQUET = "parquet"
    FORMAT_JSON = "json"
    
    # Write modes
    MODE_OVERWRITE = "overwrite"
    MODE_APPEND = "append"
    
    # Partition column
    PARTITION_COLUMN = "anomesdia"
    
    # Log format
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - [%(correlation_id)s] - %(message)s"
    
    # Required job arguments
    REQUIRED_ARGS = [
        "JOB_NAME",
        "S3_BUCKET_CORP_SOR",
        "S3_BUCKET_MF",
        "TABELA_CHECKLIST",
        "TABELA_PA_CONSOLIDADO",
        "DATA_BASE_CORP_SPEC",
        "MFPREFIX",
        "S3_BUCKET_CORP_SPEC"
    ]


class ProcessingStatus(Enum):
    """Processing status enumeration."""
    STARTED = "STARTED"
    EXTRACTING = "EXTRACTING"
    TRANSFORMING = "TRANSFORMING"
    LOADING = "LOADING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class ETLException(Exception):
    """Base exception for ETL operations."""
    pass


class DataExtractionException(ETLException):
    """Exception raised during data extraction."""
    pass


class DataTransformationException(ETLException):
    """Exception raised during data transformation."""
    pass


class DataLoadException(ETLException):
    """Exception raised during data loading."""
    pass


class ConfigurationException(ETLException):
    """Exception raised for configuration errors."""
    pass


# ============================================================================
# DATA CLASSES
# ============================================================================

class JobConfiguration(BaseModel):
    """Job configuration model with validation."""
    job_name: str = Field(..., min_length=1, description="Nome do job Glue")
    s3_bucket_corp_sor: str = Field(..., min_length=1, description="Bucket S3 corporativo SOR")
    s3_bucket_mf: str = Field(..., min_length=1, description="Bucket S3 MF")
    tabela_checklist: str = Field(..., min_length=1, description="Nome da tabela checklist")
    tabela_pa_consolidado: str = Field(..., min_length=1, description="Nome da tabela PA consolidado")
    data_base_corp_spec: str = Field(..., min_length=1, description="Database corporativo")
    mfprefix: str = Field(..., min_length=1, description="Prefixo MF")
    s3_bucket_corp_spec: str = Field(..., min_length=1, description="Bucket S3 corporativo SPEC")
    partition_date: str = Field(..., regex=r'^\d{8}$', description="Data da partição no formato YYYYMMDD")
    correlation_id: str = Field(..., min_length=1, description="ID de correlação único")
    
    @field_validator('partition_date')
    @classmethod
    def validate_partition_date(cls, v: str) -> str:
        """Valida se a data está no formato YYYYMMDD"""
        try:
            datetime.strptime(v, '%Y%m%d')
            return v
        except ValueError:
            raise ValueError('partition_date deve estar no formato YYYYMMDD')
    
    class Config:
        """Configuração do modelo Pydantic."""
        validate_assignment = True
        extra = "forbid"
        schema_extra = {
            "example": {
                "job_name": "tbfc6-etl-job",
                "s3_bucket_corp_sor": "bucket-corp-sor",
                "s3_bucket_mf": "bucket-mf",
                "tabela_checklist": "tbfc6_consig_inss_checklist",
                "tabela_pa_consolidado": "tbfc6_pa_consolidado_inss",
                "data_base_corp_spec": "database_corp_spec",
                "mfprefix": "mf_prefix",
                "s3_bucket_corp_spec": "bucket-corp-spec",
                "partition_date": "20241201",
                "correlation_id": "550e8400-e29b-41d4-a716-446655440000"
            }
        }


class ProcessingMetrics(BaseModel):
    """Processing metrics model with validation."""
    records_extracted: int = Field(default=0, ge=0, description="Número de registros extraídos")
    records_transformed: int = Field(default=0, ge=0, description="Número de registros transformados")
    records_loaded: int = Field(default=0, ge=0, description="Número de registros carregados")
    processing_time_seconds: float = Field(default=0.0, ge=0.0, description="Tempo de processamento em segundos")
    status: ProcessingStatus = Field(default=ProcessingStatus.STARTED, description="Status do processamento")
    
    @field_validator('records_transformed')
    @classmethod
    def validate_records_transformed(cls, v: int, info: ValidationInfo) -> int:
        """Valida se registros transformados não excedem extraídos"""
        if 'records_extracted' in info.data and v > info.data['records_extracted']:
            raise ValueError('records_transformed não pode exceder records_extracted')
        return v

    @field_validator('records_loaded')
    @classmethod
    def validate_records_loaded(cls, v: int, info: ValidationInfo) -> int:
        """Valida se registros carregados não excedem transformados"""
        if 'records_transformed' in info.data and v > info.data['records_transformed']:
            raise ValueError('records_loaded não pode exceder records_transformed')
        return v
    
    def get_success_rate(self) -> float:
        """Calcula a taxa de sucesso do processamento."""
        if self.records_extracted == 0:
            return 0.0
        return (self.records_loaded / self.records_extracted) * 100
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Retorna um resumo do processamento."""
        return {
            "status": self.status.value,
            "records_extracted": self.records_extracted,
            "records_transformed": self.records_transformed,
            "records_loaded": self.records_loaded,
            "processing_time_seconds": self.processing_time_seconds,
            "success_rate_percent": round(self.get_success_rate(), 2)
        }
    
    class Config:
        """Configuração do modelo Pydantic."""
        validate_assignment = True
        use_enum_values = True


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

class CorrelationFilter(logging.Filter):
    """Custom logging filter to add correlation ID."""
    
    def __init__(self, correlation_id: str):
        super().__init__()
        self.correlation_id = correlation_id
    
    def filter(self, record):
        record.correlation_id = self.correlation_id
        return True


def setup_logging(correlation_id: str) -> logging.Logger:
    """Setup logging configuration with correlation ID."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create new handler with custom format
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        Constants.LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    
    # Add correlation filter
    correlation_filter = CorrelationFilter(correlation_id)
    handler.addFilter(correlation_filter)
    
    logger.addHandler(handler)
    return logger


# ============================================================================
# SCHEMA DEFINITIONS
# ============================================================================

class SchemaDefinitions:
    """Schema definitions for data structures."""
    
    @staticmethod
    def get_checklist_schema() -> StructType:
        """Get schema for checklist table."""
        return StructType([
            StructField("cod_idef_pess", StringType(), True),
            StructField("payloadinput", StringType(), True),
            StructField("payloadoutput", StringType(), True),
            StructField("payloadoutputdmps", StringType(), True),
            StructField("dat_hor_exeo_decs", TimestampType(), True),
            StructField("nom_pilo_pltc_cred", StringType(), True),
            StructField("cod_idef_tran_sist_prod", StringType(), True),
            StructField("piloto", BooleanType(), True)
        ])
    
    @staticmethod
    def get_payload_schema() -> StructType:
        """Get schema for payload parsing."""
        return StructType([
            StructField("flagPiloto", BooleanType(), True),
            StructField("resultadoCrediario", StructType([
                StructField("carenciaMaxima", IntegerType(), True),
                StructField("carenciaMinima", IntegerType(), True),
                StructField("IndElegibTransbordo", BooleanType(), True),
                StructField("limiteCliente", DoubleType(), True),
                StructField("limiteMaxDisponivel", DoubleType(), True),
                StructField("limiteOferta", DoubleType(), True),
                StructField("parcelaDisponivel", DoubleType(), True),
                StructField("prazoMaximoFinanciamento", IntegerType(), True),
                StructField("tetoMaxCred", DoubleType(), True),
                StructField("vIrParcelaMax", DoubleType(), True),
                StructField("vIrParcelaMaxCompromisso", DoubleType(), True),
                StructField("elegibilidadeContrato", BooleanType(), True),
                StructField("prazoMinimo", IntegerType(), True),
                StructField("rendaLiquidaRecomposta", DoubleType(), True),
                StructField("margemDisponivel", DoubleType(), True),
                StructField("margemDisponivelRefin", DoubleType(), True),
                StructField("margemMaximaConsignavel", DoubleType(), True),
                StructField("elegibilidadeClienteRenov", BooleanType(), True),
                StructField("vIrBeneficioRecomposto", DoubleType(), True),
                StructField("margemDisponivelPortabilidade", DoubleType(), True),
                StructField("SUR", DoubleType(), True)
            ]), True),
            StructField("resultadoElegibilidade", BooleanType(), True),
            StructField("statusDecisao", StringType(), True),
            StructField("grupoControle", BooleanType(), True),
            StructField("flagPilotoGame", BooleanType(), True)
        ])


# ============================================================================
# AWS CLIENTS MANAGER
# ============================================================================

class AWSClientsManager:
    """Manages AWS service clients."""
    
    def __init__(self):
        self._s3_client = None
        self._athena_client = None
    
    @property
    def s3_client(self):
        """Get S3 client (lazy initialization)."""
        if self._s3_client is None:
            self._s3_client = boto3.client("s3")
        return self._s3_client
    
    @property
    def athena_client(self):
        """Get Athena client (lazy initialization)."""
        if self._athena_client is None:
            self._athena_client = boto3.client("athena")
        return self._athena_client


# ============================================================================
# MAIN ETL CLASS
# ============================================================================

class TBFC6ETLProcessor:
    """Main ETL processor for TBFC6 Consig INSS data."""
    
    def __init__(self, config: JobConfiguration, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.aws_clients = AWSClientsManager()
        self.metrics = ProcessingMetrics()
        self.spark: Optional[SparkSession] = None
        self.glue_context: Optional[GlueContext] = None
        self.job: Optional[Job] = None
    
    def initialize_spark_glue(self) -> Tuple[SparkSession, GlueContext, Job]:
        """Initialize Spark and Glue context."""
        try:
            self.logger.info("Initializing Spark and Glue context")
            
            conf = SparkConf().setAppName(self.config.job_name)
            sc = SparkContext(conf=conf)
            
            self.glue_context = GlueContext(sc)
            self.spark = self.glue_context.spark_session
            self.job = Job(self.glue_context)
            self.job.init(self.config.job_name, {})
            
            self.logger.info("Spark and Glue context initialized successfully")
            return self.spark, self.glue_context, self.job
            
        except Exception as e:
            raise ConfigurationException(f"Failed to initialize Spark/Glue: {str(e)}")
    
    def extract_checklist_data(self) -> DataFrame:
        """Extract data from checklist table filtering by piloto=true."""
        try:
            self.metrics.status = ProcessingStatus.EXTRACTING
            self.logger.info(f"Starting data extraction from {self.config.tabela_checklist}")
            
            # Find latest folder
            prefix = self.config.tabela_checklist
            if not prefix.endswith("/"):
                prefix += "/"
            
            s3_path = self._find_latest_folder(self.config.s3_bucket_corp_sor, prefix)
            if not s3_path:
                raise DataExtractionException(f"No data found for prefix: {prefix}")
            
            self.logger.info(f"Found S3 path: {s3_path}")
            
            # Read data with schema
            df = self._read_data_with_fallback(s3_path)
            
            # Filter by piloto = true
            df_filtered = df.filter(F.col("piloto") == True)
            
            self.metrics.records_extracted = df_filtered.count()
            self.logger.info(f"Extracted {self.metrics.records_extracted} records with piloto=true")
            
            if self.metrics.records_extracted == 0:
                self.logger.warning("No records found with piloto=true")
            
            return df_filtered
            
        except Exception as e:
            raise DataExtractionException(f"Failed to extract checklist data: {str(e)}")
    
    def transform_checklist_data(self, df: DataFrame) -> DataFrame:
        """Transform checklist data by parsing payloadoutputdmps."""
        try:
            self.metrics.status = ProcessingStatus.TRANSFORMING
            self.logger.info("Starting data transformation")
            
            # Validate required columns
            required_columns = ["payloadoutputdmps", "cod_idef_pess"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise DataTransformationException(f"Missing required columns: {missing_columns}")
            
            # Parse JSON payload
            payload_schema = SchemaDefinitions.get_payload_schema()
            df_with_payload = df.withColumn(
                "payload_parsed",
                F.from_json(F.col("payloadoutputdmps"), payload_schema)
            )
            
            # Extract and flatten payload fields
            df_transformed = df_with_payload.select(
                F.col("cod_idef_pess"),
                F.col("dat_hor_exeo_decs"),
                F.col("nom_pilo_pltc_cred"),
                F.col("cod_idef_tran_sist_prod"),
                
                # Payload root fields
                F.col("payload_parsed.flagPiloto").alias("flag_piloto"),
                F.col("payload_parsed.resultadoElegibilidade").alias("resultado_elegibilidade"),
                F.col("payload_parsed.statusDecisao").alias("status_decisao"),
                F.col("payload_parsed.grupoControle").alias("grupo_controle"),
                F.col("payload_parsed.flagPilotoGame").alias("flag_piloto_game"),
                
                # Resultado Creditario fields
                F.col("payload_parsed.resultadoCrediario.carenciaMaxima").alias("carencia_maxima"),
                F.col("payload_parsed.resultadoCrediario.carenciaMinima").alias("carencia_minima"),
                F.col("payload_parsed.resultadoCrediario.IndElegibTransbordo").alias("ind_elegib_transbordo"),
                F.col("payload_parsed.resultadoCrediario.limiteCliente").alias("limite_cliente"),
                F.col("payload_parsed.resultadoCrediario.limiteMaxDisponivel").alias("limite_max_disponivel"),
                F.col("payload_parsed.resultadoCrediario.limiteOferta").alias("limite_oferta"),
                F.col("payload_parsed.resultadoCrediario.parcelaDisponivel").alias("parcela_disponivel"),
                F.col("payload_parsed.resultadoCrediario.prazoMaximoFinanciamento").alias("prazo_maximo_financiamento"),
                F.col("payload_parsed.resultadoCrediario.tetoMaxCred").alias("teto_max_cred"),
                F.col("payload_parsed.resultadoCrediario.vIrParcelaMax").alias("v_ir_parcela_max"),
                F.col("payload_parsed.resultadoCrediario.vIrParcelaMaxCompromisso").alias("v_ir_parcela_max_compromisso"),
                F.col("payload_parsed.resultadoCrediario.elegibilidadeContrato").alias("elegibilidade_contrato"),
                F.col("payload_parsed.resultadoCrediario.prazoMinimo").alias("prazo_minimo"),
                F.col("payload_parsed.resultadoCrediario.rendaLiquidaRecomposta").alias("renda_liquida_recomposta"),
                F.col("payload_parsed.resultadoCrediario.margemDisponivel").alias("margem_disponivel"),
                F.col("payload_parsed.resultadoCrediario.margemDisponivelRefin").alias("margem_disponivel_refin"),
                F.col("payload_parsed.resultadoCrediario.margemMaximaConsignavel").alias("margem_maxima_consignavel"),
                F.col("payload_parsed.resultadoCrediario.elegibilidadeClienteRenov").alias("elegibilidade_cliente_renov"),
                F.col("payload_parsed.resultadoCrediario.vIrBeneficioRecomposto").alias("v_ir_beneficio_recomposto"),
                F.col("payload_parsed.resultadoCrediario.margemDisponivelPortabilidade").alias("margem_disponivel_portabilidade"),
                F.col("payload_parsed.resultadoCrediario.SUR").alias("sur"),
                
                # Metadata fields
                F.lit(self.config.correlation_id).alias("correlation_id"),
                F.current_timestamp().alias("data_processamento"),
                F.lit("ETL_GLUE").alias("origem_processamento"),
                F.lit(self.config.partition_date).alias(Constants.PARTITION_COLUMN),
                
                # Keep original payload for audit
                F.col("payloadoutputdmps").alias("payload_original")
            )
            
            self.metrics.records_transformed = df_transformed.count()
            self.logger.info(f"Transformed {self.metrics.records_transformed} records successfully")
            
            return df_transformed
            
        except Exception as e:
            raise DataTransformationException(f"Failed to transform data: {str(e)}")
    
    def create_pa_consolidado_structure(self, df: DataFrame) -> DataFrame:
        """Create PA consolidado table structure."""
        try:
            self.logger.info("Creating PA consolidado structure")
            
            # Select and rename columns for PA consolidado table
            df_pa = df.select(
                F.col("cod_idef_pess"),
                F.col("dat_hor_exeo_decs"),
                F.col("nom_pilo_pltc_cred"),
                F.col("cod_idef_tran_sist_prod"),
                F.col("flag_piloto"),
                F.col("resultado_elegibilidade"),
                F.col("status_decisao"),
                F.col("grupo_controle"),
                F.col("flag_piloto_game"),
                F.col("carencia_maxima"),
                F.col("carencia_minima"),
                F.col("ind_elegib_transbordo"),
                F.col("limite_cliente"),
                F.col("limite_max_disponivel"),
                F.col("limite_oferta"),
                F.col("parcela_disponivel"),
                F.col("prazo_maximo_financiamento"),
                F.col("teto_max_cred"),
                F.col("v_ir_parcela_max"),
                F.col("v_ir_parcela_max_compromisso"),
                F.col("elegibilidade_contrato"),
                F.col("prazo_minimo"),
                F.col("renda_liquida_recomposta"),
                F.col("margem_disponivel"),
                F.col("margem_disponivel_refin"),
                F.col("margem_maxima_consignavel"),
                F.col("elegibilidade_cliente_renov"),
                F.col("v_ir_beneficio_recomposto"),
                F.col("margem_disponivel_portabilidade"),
                F.col("sur"),
                F.col("correlation_id"),
                F.col("data_processamento"),
                F.col("origem_processamento"),
                F.col(Constants.PARTITION_COLUMN)
            )
            
            record_count = df_pa.count()
            self.logger.info(f"PA consolidado structure created with {record_count} records")
            
            return df_pa
            
        except Exception as e:
            raise DataTransformationException(f"Failed to create PA consolidado structure: {str(e)}")
    
    def create_payload_audit_table(self, df: DataFrame) -> DataFrame:
        """Create payload audit table for tracking."""
        try:
            self.logger.info("Creating payload audit table")
            
            audit_df = df.select(
                F.col("cod_idef_pess"),
                F.col("payload_original").alias("payload_content"),
                F.col("correlation_id"),
                F.col("data_processamento").alias("created_at"),
                F.col(Constants.PARTITION_COLUMN)
            )
            
            record_count = audit_df.count()
            self.logger.info(f"Payload audit table created with {record_count} records")
            
            return audit_df
            
        except Exception as e:
            raise DataTransformationException(f"Failed to create payload audit table: {str(e)}")
    
    def load_data_to_s3(self, df: DataFrame, table_name: str) -> None:
        """Load data to S3 with retry mechanism."""
        try:
            self.metrics.status = ProcessingStatus.LOADING
            
            s3_path = f"s3://{self.config.s3_bucket_corp_spec}/{table_name}/{Constants.PARTITION_COLUMN}={self.config.partition_date}"
            self.logger.info(f"Loading data to S3: {s3_path}")
            
            for attempt in range(1, Constants.MAX_RETRIES + 1):
                try:
                    df.write \
                        .format(Constants.FORMAT_PARQUET) \
                        .mode(Constants.MODE_OVERWRITE) \
                        .save(s3_path)
                    
                    self.logger.info(f"Data successfully loaded to S3: {s3_path}")
                    break
                    
                except Exception as e:
                    if attempt == Constants.MAX_RETRIES:
                        raise DataLoadException(f"Failed to load data after {Constants.MAX_RETRIES} attempts: {str(e)}")
                    
                    self.logger.warning(f"Attempt {attempt}/{Constants.MAX_RETRIES} failed: {str(e)}. Retrying...")
                    
        except Exception as e:
            raise DataLoadException(f"Failed to load data to S3: {str(e)}")
    
    def update_athena_partitions(self, table_name: str) -> None:
        """Update Athena partitions for the loaded data."""
        try:
            self.logger.info(f"Updating Athena partitions for table: {table_name}")
            
            database = self.config.data_base_corp_spec
            s3_location = f"s3://{self.config.s3_bucket_corp_spec}/{table_name}/{Constants.PARTITION_COLUMN}={self.config.partition_date}/"
            
            query = f"""
                ALTER TABLE {database}.{table_name}
                ADD IF NOT EXISTS PARTITION ({Constants.PARTITION_COLUMN}='{self.config.partition_date}')
                LOCATION '{s3_location}'
            """
            
            self.logger.info(f"Executing Athena query: {query}")
            
            response = self.aws_clients.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={
                    "OutputLocation": f"s3://{self.config.s3_bucket_corp_spec}/athena_query_results/"
                }
            )
            
            query_execution_id = response["QueryExecutionId"]
            self.logger.info(f"Athena query started with ID: {query_execution_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to update Athena partitions: {str(e)}")
            # Don't raise exception for partition updates as it's not critical
    
    def cleanup_temp_files(self) -> None:
        """Clean up temporary files in S3."""
        try:
            self.logger.info("Cleaning up temporary files")
            
            temp_prefix = f"{self.config.tabela_checklist}_temp/"
            
            response = self.aws_clients.s3_client.list_objects_v2(
                Bucket=self.config.s3_bucket_mf,
                Prefix=temp_prefix
            )
            
            if "Contents" in response:
                for obj in response["Contents"]:
                    self.aws_clients.s3_client.delete_object(
                        Bucket=self.config.s3_bucket_mf,
                        Key=obj["Key"]
                    )
                    self.logger.info(f"Deleted temporary file: {obj['Key']}")
            
            self.logger.info(f"Temporary folder cleaned: {temp_prefix}")
            
        except Exception as e:
            self.logger.warning(f"Failed to cleanup temporary files: {str(e)}")
            # Don't raise exception for cleanup as it's not critical
    
    def _read_data_with_fallback(self, s3_path: str) -> DataFrame:
        """Read data with multiple format fallbacks."""
        schema = SchemaDefinitions.get_checklist_schema()
        
        # Try JSON with schema first
        try:
            self.logger.info("Attempting to read data as JSON with schema")
            df = self.spark.read.schema(schema).option("multiline", "true").json(s3_path)
            if df.count() > 0:
                return df
        except Exception as e:
            self.logger.warning(f"Failed to read as JSON with schema: {str(e)}")
        
        # Try JSON without schema
        try:
            self.logger.info("Attempting to read data as JSON without schema")
            df = self.spark.read.option("multiline", "true").json(s3_path)
            if df.count() > 0:
                return df
        except Exception as e:
            self.logger.warning(f"Failed to read as JSON without schema: {str(e)}")
        
        # Try Parquet
        try:
            self.logger.info("Attempting to read data as Parquet")
            df = self.spark.read.parquet(s3_path)
            if df.count() > 0:
                return df
        except Exception as e:
            self.logger.warning(f"Failed to read as Parquet: {str(e)}")
        
        raise DataExtractionException(f"Unable to read data from path: {s3_path}")
    
    def _find_latest_folder(self, bucket: str, prefix: str) -> Optional[str]:
        """Find the latest folder in S3 based on prefix."""
        try:
            folders = self._list_s3_folders(bucket, prefix)
            if not folders:
                return None
            
            latest_folder = max(folders)
            return f"s3://{bucket}/{latest_folder}"
            
        except Exception as e:
            self.logger.error(f"Failed to find latest folder: {str(e)}")
            return None
    
    def _list_s3_folders(self, bucket: str, prefix: str) -> List[str]:
        """List folders in S3 bucket with given prefix."""
        folders = []
        continuation_token = None
        
        while True:
            try:
                kwargs = {
                    "Bucket": bucket,
                    "Prefix": prefix,
                    "Delimiter": "/"
                }
                
                if continuation_token:
                    kwargs["ContinuationToken"] = continuation_token
                
                response = self.aws_clients.s3_client.list_objects_v2(**kwargs)
                
                folders.extend([
                    content["Prefix"] for content in response.get("CommonPrefixes", [])
                ])
                
                if not response.get("IsTruncated"):
                    break
                
                continuation_token = response.get("NextContinuationToken")
                
            except Exception as e:
                self.logger.error(f"Failed to list S3 folders: {str(e)}")
                break
        
        return folders
    
    def run(self) -> ProcessingMetrics:
        """Execute the complete ETL process."""
        start_time = datetime.now()
        
        try:
            self.logger.info("Starting TBFC6 ETL process")
            
            # Initialize Spark and Glue
            self.initialize_spark_glue()
            
            # Extract data
            df_checklist = self.extract_checklist_data()
            
            # Transform data
            df_transformed = self.transform_checklist_data(df_checklist)
            
            # Create PA consolidado structure
            df_pa_consolidado = self.create_pa_consolidado_structure(df_transformed)
            
            # Create payload audit table
            df_payload_audit = self.create_payload_audit_table(df_transformed)
            
            # Load data to S3
            self.load_data_to_s3(df_pa_consolidado, self.config.tabela_pa_consolidado)
            self.load_data_to_s3(df_payload_audit, "payloadoutputdmps")
            
            self.metrics.records_loaded = df_pa_consolidado.count()
            
            # Update Athena partitions
            self.update_athena_partitions(self.config.tabela_pa_consolidado)
            self.update_athena_partitions("payloadoutputdmps")
            
            # Cleanup
            self.cleanup_temp_files()
            
            # Calculate processing time
            end_time = datetime.now()
            self.metrics.processing_time_seconds = (end_time - start_time).total_seconds()
            self.metrics.status = ProcessingStatus.COMPLETED
            
            self.logger.info(f"ETL process completed successfully in {self.metrics.processing_time_seconds:.2f} seconds")
            self.logger.info(f"Processing metrics: {self.metrics.get_processing_summary()}")
            
            return self.metrics
            
        except Exception as e:
            self.metrics.status = ProcessingStatus.FAILED
            self.logger.error(f"ETL process failed: {str(e)}", exc_info=True)
            raise
        
        finally:
            if self.job:
                self.job.commit()
            if self.spark:
                self.spark.stop()


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_job_arguments() -> Dict[str, str]:
    """Get and validate job arguments."""
    try:
        args = getResolvedOptions(sys.argv, Constants.REQUIRED_ARGS)
        
        # Validate required arguments
        missing_args = [arg for arg in Constants.REQUIRED_ARGS if not args.get(arg)]
        if missing_args:
            raise ConfigurationException(f"Missing required arguments: {missing_args}")
        
        return args
        
    except Exception as e:
        raise ConfigurationException(f"Failed to get job arguments: {str(e)}")


def create_job_configuration(args: Dict[str, str]) -> JobConfiguration:
    """Create job configuration from arguments with Pydantic validation."""
    correlation_id = str(uuid.uuid4())
    partition_date = datetime.now().strftime("%Y%m%d")
    
    try:
        config = JobConfiguration(
            job_name=args["JOB_NAME"],
            s3_bucket_corp_sor=args["S3_BUCKET_CORP_SOR"],
            s3_bucket_mf=args["S3_BUCKET_MF"],
            tabela_checklist=args["TABELA_CHECKLIST"],
            tabela_pa_consolidado=args["TABELA_PA_CONSOLIDADO"],
            data_base_corp_spec=args["DATA_BASE_CORP_SPEC"],
            mfprefix=args["MFPREFIX"],
            s3_bucket_corp_spec=args["S3_BUCKET_CORP_SPEC"],
            partition_date=partition_date,
            correlation_id=correlation_id
        )
        return config
    except Exception as e:
        raise ConfigurationException(f"Failed to create job configuration: {str(e)}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main() -> None:
    """Main execution function."""
    config = None
    logger = None
    
    try:
        # Get job arguments
        args = get_job_arguments()
        
        # Create configuration
        config = create_job_configuration(args)
        
        # Setup logging
        logger = setup_logging(config.correlation_id)
        
        logger.info(f"Starting ETL job: {config.job_name}")
        logger.info(f"Configuration: {config}")
        
        # Create and run ETL processor
        etl_processor = TBFC6ETLProcessor(config, logger)
        metrics = etl_processor.run()
        
        logger.info(f"ETL job completed successfully: {metrics.get_processing_summary()}")
        
    except Exception as e:
        if logger:
            logger.error(f"ETL job failed: {str(e)}", exc_info=True)
        else:
            print(f"ETL job failed: {str(e)}")
        
        # Re-raise the exception to ensure Glue job fails
        raise


if __name__ == "__main__":
    main()
