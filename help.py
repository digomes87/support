"""
Validação de Dados e Funções Utilitárias

Este módulo fornece funções utilitárias para conversão segura de tipos de dados,
filtragem de partições e validação de dados para o processo ETL.
"""

from typing import Any, Dict, List, Union

from pyspark.sql.types import DecimalType, IntegerType, StringType

# Mapeamentos de tipos de coluna para validação de dados
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
    Gera expressão SQL de conversão numérica segura.
    
    Args:
        column_name: Nome da coluna a ser convertida
        target_type: Tipo de dados de destino (STRING, INT, DECIMAL, etc.)
        alias: Alias para a coluna resultante
    
    Returns:
        Expressão SQL CAST com tratamento de nulos
    """
    return f"CASE WHEN {column_name} IS NULL THEN CAST(-1 AS {target_type}) ELSE CAST({column_name} AS {target_type}) END AS {alias}"


def safe_numeric_where_condition(column_name: str, exclude_value: Union[int, float]) -> str:
    """
    Gera condição WHERE segura para colunas numéricas.
    
    Args:
        column_name: Nome da coluna
        exclude_value: Valor a ser excluído dos resultados
    
    Returns:
        Condição SQL WHERE com tratamento de nulos
    """
    return f"({column_name} IS NULL OR {column_name} != {exclude_value})"


def get_partition_filter(database_name: str, table_name: str, glue_client) -> str:
    """
    Obtém filtro de partição para a partição mais recente de uma tabela.
    
    Args:
        database_name: Nome do banco de dados
        table_name: Nome da tabela
        glue_client: Instância do cliente AWS Glue
    
    Returns:
        String de filtro de partição para a partição mais recente
    """
    try:
        # Obtém partições da tabela do catálogo Glue
        response = glue_client.get_partitions(
            DatabaseName=database_name,
            TableName=table_name,
            MaxResults=1000
        )
        
        partitions = response.get('Partitions', [])
        
        if not partitions:
            return ""
        
        # Encontra a partição mais recente baseada nos valores de partição
        latest_partition = max(partitions, key=lambda p: p.get('Values', []))
        partition_keys = latest_partition.get('StorageDescriptor', {}).get('Columns', [])
        partition_values = latest_partition.get('Values', [])
        
        if not partition_keys or not partition_values:
            return ""
        
        # Constrói filtro de partição
        filter_conditions = []
        for i, key_info in enumerate(partition_keys):
            if i < len(partition_values):
                key_name = key_info.get('Name', '')
                key_value = partition_values[i]
                filter_conditions.append(f"{key_name} = '{key_value}'")
        
        return " AND ".join(filter_conditions)
        
    except Exception:
        # Retorna string vazia se a filtragem de partição falhar
        return ""


class JSONSchemaValidator:
    """
    Validador de Schema JSON para validação do formato de saída ETL.
    """
    
    def __init__(self):
        """Inicializa o validador com o schema esperado"""
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
        Valida estrutura de dados contra o schema esperado.
        
        Args:
            data: Dicionário de dados para validar
        
        Returns:
            Tupla de (é_válido, mensagens_de_erro)
        """
        errors = []
        
        try:
            # Validação básica de estrutura
            if not isinstance(data, dict):
                errors.append("Dados devem ser um dicionário")
                return False, errors
            
            # Verifica campos obrigatórios de nível superior
            required_fields = ["payloadIdentification", "payloadInput", "payloadAudit"]
            for field in required_fields:
                if field not in data:
                    errors.append(f"Campo obrigatório ausente: {field}")
            
            # Valida payloadIdentification
            if "payloadIdentification" in data:
                payload_id = data["payloadIdentification"]
                if not isinstance(payload_id, dict):
                    errors.append("payloadIdentification deve ser um dicionário")
                else:
                    required_id_fields = [
                        "nom_fncd_serv_nego", "cod_idef_prpt_sist_prod",
                        "cod_idef_tran_sist_cred", "cod_idef_pess"
                    ]
                    for field in required_id_fields:
                        if field not in payload_id:
                            errors.append(f"Campo obrigatório ausente em payloadIdentification: {field}")
            
            # Valida payloadInput
            if "payloadInput" in data:
                payload_input = data["payloadInput"]
                if not isinstance(payload_input, dict):
                    errors.append("payloadInput deve ser um dicionário")
                else:
                    if "proponente" not in payload_input:
                        errors.append("Campo obrigatório ausente em payloadInput: proponente")
                    elif "numeroDocumento" not in payload_input["proponente"]:
                        errors.append("Campo obrigatório ausente em proponente: numeroDocumento")
            
            # Valida payloadAudit
            if "payloadAudit" in data:
                payload_audit = data["payloadAudit"]
                if not isinstance(payload_audit, dict):
                    errors.append("payloadAudit deve ser um dicionário")
                else:
                    required_audit_fields = ["auditSteps", "calculatedVars", "isAuditEnabled", "isCalculatedVarsEnabled"]
                    for field in required_audit_fields:
                        if field not in payload_audit:
                            errors.append(f"Campo obrigatório ausente em payloadAudit: {field}")
            
            return len(errors) == 0, errors
            
        except Exception as e:
            errors.append(f"Erro de validação: {str(e)}")
            return False, errors


def validate_etl_output_format(data: Dict[str, Any]) -> tuple[bool, List[str]]:
    """
    Valida formato de saída ETL usando JSONSchemaValidator.
    
    Args:
        data: Dicionário de dados para validar
    
    Returns:
        Tupla de (é_válido, mensagens_de_erro)
    """
    validator = JSONSchemaValidator()
    return validator.validate_structure(data)


def validate_dataframe_columns(df, required_columns: List[str], logger=None) -> bool:
    """
    Valida se o DataFrame contém todas as colunas obrigatórias.
    
    Args:
        df: DataFrame Spark para validar
        required_columns: Lista de nomes de colunas obrigatórias
        logger: Instância opcional de logger
    
    Returns:
        True se todas as colunas obrigatórias estiverem presentes, False caso contrário
    """
    if df is None:
        if logger:
            logger.error("DataFrame é None")
        return False
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        if logger:
            logger.error(f"Colunas obrigatórias ausentes: {missing_columns}")
        return False
    
    return True


def log_dataframe_info(df, table_name: str, logger=None):
    """
    Registra informações básicas sobre um DataFrame.
    
    Args:
        df: DataFrame Spark
        table_name: Nome da tabela/fonte
        logger: Instância opcional de logger
    """
    if logger and df is not None:
        try:
            row_count = df.count()
            column_count = len(df.columns)
            logger.info(f"Tabela {table_name}: {row_count} linhas, {column_count} colunas")
        except Exception as e:
            logger.warning(f"Não foi possível obter informações para a tabela {table_name}: {str(e)}")
