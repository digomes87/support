"""
Data Validation and Utility Functions

This module provides utility functions for safe data type casting, 
partition filtering, and data validation for the ETL process.
"""

import json
from typing import Any, Dict, List, Optional, Union
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, DecimalType, 
    BooleanType, DateType, TimestampType
)


# Column type mappings for data validation
COLUMN_TYPE = {
    "cod_idef_pess": StringType(),
    "CorrentistaAntigo": IntegerType(),
    "Claro_Nao": IntegerType(),
    "Cod_visao_Cliente": IntegerType(),
    "Modalidade_Operacao": IntegerType(),
    "Parcela_Mercado": DecimalType(10, 2),
    "tipoInstituicao": StringType(),
    "Situacao_BX": IntegerType(),
    "Apontamento_BX": StringType(),
    "Idade": StringType(),
    "Indicador_SPO": StringType(),
    "Cross_Consignado": IntegerType(),
    "Gene_Emprego": IntegerType(),
    "Target": IntegerType(),
    "Codigo_Exclusao_Comercial": StringType(),
    "Vinculo": StringType(),
    "Cargos": StringType(),
    "Renda_Sispag": DecimalType(15, 2),
    "Valor_Contratado_Interno": DecimalType(15, 2),
    "Numero_Conveio": StringType(),
    "pct_maxi_cpmm_rend": DecimalType(5, 2),
    "qtpzmax": IntegerType(),
    "Parcela_Elegivel_Refin": DecimalType(15, 2),
    "Indicador_SPI": StringType()
}


def safe_numeric_cast(column_name: str, target_type: str, alias: str) -> str:
    """
    Generate safe numeric casting SQL expression.
    
    Args:
        column_name: Name of the column to cast
        target_type: Target data type (STRING, INT, DECIMAL, etc.)
        alias: Alias for the resulting column
    
    Returns:
        SQL CAST expression with null handling
    """
    return f"CASE WHEN {column_name} IS NULL THEN CAST(-1 AS {target_type}) ELSE CAST({column_name} AS {target_type}) END AS {alias}"


def safe_numeric_where_condition(column_name: str, exclude_value: Union[int, float]) -> str:
    """
    Generate safe WHERE condition for numeric columns.
    
    Args:
        column_name: Name of the column
        exclude_value: Value to exclude from results
    
    Returns:
        SQL WHERE condition with null handling
    """
    return f"({column_name} IS NULL OR {column_name} != {exclude_value})"


def get_partition_filter(database_name: str, table_name: str, glue_client) -> str:
    """
    Get partition filter for the latest partition of a table.
    
    Args:
        database_name: Name of the database
        table_name: Name of the table
        glue_client: AWS Glue client instance
    
    Returns:
        Partition filter string for the latest partition
    """
    try:
        # Get table partitions from Glue catalog
        response = glue_client.get_partitions(
            DatabaseName=database_name,
            TableName=table_name,
            MaxResults=1000
        )
        
        partitions = response.get('Partitions', [])
        
        if not partitions:
            return ""
        
        # Find the latest partition based on partition values
        latest_partition = max(partitions, key=lambda p: p.get('Values', []))
        partition_keys = latest_partition.get('StorageDescriptor', {}).get('Columns', [])
        partition_values = latest_partition.get('Values', [])
        
        if not partition_keys or not partition_values:
            return ""
        
        # Build partition filter
        filter_conditions = []
        for i, key_info in enumerate(partition_keys):
            if i < len(partition_values):
                key_name = key_info.get('Name', '')
                key_value = partition_values[i]
                filter_conditions.append(f"{key_name} = '{key_value}'")
        
        return " AND ".join(filter_conditions)
        
    except Exception as e:
        # Return empty string if partition filtering fails
        return ""


class JSONSchemaValidator:
    """
    JSON Schema Validator for ETL output format validation.
    """
    
    def __init__(self):
        """Initialize the validator with expected schema"""
        self.expected_schema = {
            "type": "object",
            "required": ["payloadIdentification", "payloadInput", "payloadAudit"],
            "properties": {
                "payloadIdentification": {
                    "type": "object",
                    "required": [
                        "nom_fncd_serv_nego", "cod_idef_prpt_sist_prod", 
                        "cod_idef_tran_sist_cred", "cod_idef_job_jorn",
                        "cod_idef_tran_sist_prod", "cod_idef_prso_nego",
                        "cod_idef_pess", "cod_tipo_pess"
                    ],
                    "properties": {
                        "nom_fncd_serv_nego": {"type": "string"},
                        "cod_idef_prpt_sist_prod": {"type": "string"},
                        "cod_idef_tran_sist_cred": {"type": "string"},
                        "cod_idef_job_jorn": {"type": "string"},
                        "cod_idef_tran_sist_prod": {"type": "string"},
                        "cod_idef_prso_nego": {"type": "string"},
                        "cod_idef_pess": {"type": "string"},
                        "nom_prso_sist_cred": {"type": "string"},
                        "cod_stat_decs": {"type": "string"},
                        "nom_tipo_decs_prpt": {"type": "string"},
                        "nom_moto_anal_prpt": {"type": "string"},
                        "cod_tipo_pess": {"type": "string"},
                        "nom_prod_cred": {"type": "string"},
                        "nom_fase_cred": {"type": "string"},
                        "nom_pilo_pltc_cred": {"type": "string"},
                        "cod_vers_pltc": {"type": "string"},
                        "cod_vers_tecn_pltc": {"type": "string"},
                        "dat_hor_exeo_descs": {"type": "string"}
                    }
                },
                "payloadInput": {
                    "type": "object",
                    "required": ["tipoProcesso", "solicitacao", "proponente"],
                    "properties": {
                        "tipoProcesso": {"type": "string"},
                        "solicitacao": {
                            "type": "object",
                            "properties": {
                                "codCanal": {"type": "integer"},
                                "codsubCanal": {"type": "integer"},
                                "numeroConvenio": {"type": "string"}
                            }
                        },
                        "dadosOferta": {
                            "type": "object",
                            "properties": {
                                "flagInelebilidadeForte": {"type": "integer"}
                            }
                        },
                        "listaValoresCalculados": {
                            "type": "object",
                            "properties": {
                                "percentual": {"type": "number"},
                                "prazo": {"type": "integer"},
                                "vlrParcelaElegivel": {"type": "number"}
                            }
                        },
                        "proponente": {
                            "type": "object",
                            "required": ["numeroDocumento"],
                            "properties": {
                                "numeroDocumento": {"type": "string"},
                                "listaContratos": {"type": "array"},
                                "dadosEndividamento": {
                                    "type": "object",
                                    "properties": {
                                        "valorParcConsignado": {"type": "number"},
                                        "valorParcConsignadoItau": {"type": "number"}
                                    }
                                },
                                "dadosFatorRisco": {
                                    "type": "object",
                                    "properties": {
                                        "listaExclusao": {"type": "array"},
                                        "listaApontamentos": {"type": "array"},
                                        "geneEmprego": {"type": "integer"}
                                    }
                                },
                                "indCorrentistaAntigo": {
                                    "type": "object",
                                    "properties": {
                                        "indCorrentistaAntigo": {"type": "integer"}
                                    }
                                },
                                "dadosBacen": {
                                    "type": "object",
                                    "properties": {
                                        "codSubModalidadeOperacao": {"type": "integer"},
                                        "tipoInstituicao": {"type": "string"}
                                    }
                                },
                                "dadosPessoaFisica": {
                                    "type": "object",
                                    "properties": {
                                        "idadeCliente": {"type": "string"},
                                        "spiVisaoConta": {"type": "string"},
                                        "spoVisaoConta": {"type": "string"},
                                        "listaVinculos": {"type": "array"},
                                        "listaCargos": {"type": "array"}
                                    }
                                },
                                "dadosVisaoCliente": {
                                    "type": "object",
                                    "properties": {
                                        "rating": {"type": "integer"}
                                    }
                                },
                                "dadosModelos": {
                                    "type": "object",
                                    "properties": {
                                        "crossConsignado": {"type": "integer"},
                                        "visaoCliente": {"type": "integer"},
                                        "geneEmprego": {"type": "integer"}
                                    }
                                },
                                "dadosRenda": {
                                    "type": "object",
                                    "properties": {
                                        "rendaInternaCliente": {"type": "number"}
                                    }
                                }
                            }
                        }
                    }
                },
                "payloadAudit": {
                    "type": "object",
                    "required": ["auditSteps", "calculatedVars", "isAuditEnabled", "isCalculatedVarsEnabled"],
                    "properties": {
                        "auditSteps": {"type": "array"},
                        "calculatedVars": {"type": "array"},
                        "isAuditEnabled": {"type": "boolean"},
                        "isCalculatedVarsEnabled": {"type": "boolean"}
                    }
                }
            }
        }
    
    def validate_structure(self, data: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate data structure against expected schema.
        
        Args:
            data: Data dictionary to validate
        
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        try:
            # Basic structure validation
            if not isinstance(data, dict):
                errors.append("Data must be a dictionary")
                return False, errors
            
            # Check required top-level fields
            required_fields = ["payloadIdentification", "payloadInput", "payloadAudit"]
            for field in required_fields:
                if field not in data:
                    errors.append(f"Missing required field: {field}")
            
            # Validate payloadIdentification
            if "payloadIdentification" in data:
                payload_id = data["payloadIdentification"]
                if not isinstance(payload_id, dict):
                    errors.append("payloadIdentification must be a dictionary")
                else:
                    required_id_fields = [
                        "nom_fncd_serv_nego", "cod_idef_prpt_sist_prod",
                        "cod_idef_tran_sist_cred", "cod_idef_pess"
                    ]
                    for field in required_id_fields:
                        if field not in payload_id:
                            errors.append(f"Missing required field in payloadIdentification: {field}")
            
            # Validate payloadInput
            if "payloadInput" in data:
                payload_input = data["payloadInput"]
                if not isinstance(payload_input, dict):
                    errors.append("payloadInput must be a dictionary")
                else:
                    if "proponente" not in payload_input:
                        errors.append("Missing required field in payloadInput: proponente")
                    elif "numeroDocumento" not in payload_input["proponente"]:
                        errors.append("Missing required field in proponente: numeroDocumento")
            
            # Validate payloadAudit
            if "payloadAudit" in data:
                payload_audit = data["payloadAudit"]
                if not isinstance(payload_audit, dict):
                    errors.append("payloadAudit must be a dictionary")
                else:
                    required_audit_fields = ["auditSteps", "calculatedVars", "isAuditEnabled", "isCalculatedVarsEnabled"]
                    for field in required_audit_fields:
                        if field not in payload_audit:
                            errors.append(f"Missing required field in payloadAudit: {field}")
            
            return len(errors) == 0, errors
            
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
            return False, errors


def validate_etl_output_format(data: Dict[str, Any]) -> tuple[bool, List[str]]:
    """
    Validate ETL output format using JSONSchemaValidator.
    
    Args:
        data: Data dictionary to validate
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    validator = JSONSchemaValidator()
    return validator.validate_structure(data)


def validate_dataframe_columns(df, required_columns: List[str], logger=None) -> bool:
    """
    Validate that DataFrame contains all required columns.
    
    Args:
        df: Spark DataFrame to validate
        required_columns: List of required column names
        logger: Optional logger instance
    
    Returns:
        True if all required columns are present, False otherwise
    """
    if df is None:
        if logger:
            logger.error("DataFrame is None")
        return False
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        if logger:
            logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    return True


def log_dataframe_info(df, table_name: str, logger=None):
    """
    Log basic information about a DataFrame.
    
    Args:
        df: Spark DataFrame
        table_name: Name of the table/source
        logger: Optional logger instance
    """
    if logger and df is not None:
        try:
            row_count = df.count()
            column_count = len(df.columns)
            logger.info(f"Table {table_name}: {row_count} rows, {column_count} columns")
        except Exception as e:
            logger.warning(f"Could not get info for table {table_name}: {str(e)}")
