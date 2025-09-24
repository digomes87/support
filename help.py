"""
Configuração de mapeamento de banco de dados para operações ETL.
Centraliza todos os mapeamentos de tabelas de banco de dados, especificações de colunas e utilitários de consulta.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class TableMapping:
    """Configuração para mapeamento de tabela de banco de dados."""
    database: str
    table: str
    column_mapping: Dict[str, str]
    conditions: Optional[List[str]] = None


class DatabaseMappingConfig:
    """Configuração central para todos os mapeamentos e consultas de banco de dados."""
    
    def __init__(self):
        """Inicializa mapeamentos e configurações de banco de dados."""
        self.table_mappings = {
            # Informações de colaboração de convênio
            "conv_cola_info": TableMapping(
                database="db_corp_servicosdecontratacao_consignado_sor_01",
                table="tbazcma",
                column_mapping={
                    "nrmaer": "nrmaer",
                    "cdgrcons": "cdgrcons", 
                    "tpempseg": "tpempseg",
                    "cdagecdt": "cdagecdt",
                    "cdctacdt": "cdctacdt"
                },
                conditions=["tpempseg = 2", "cdgrcons != 'INSS'"]
            ),
            
            # Informações de contrato de funcionário
            "employee_contract_info": TableMapping(
                database="db_corp_servicosdecontratacao_consignado_sor_01",
                table="tbazctc",
                column_mapping={
                    "nrmaer": "nrmaer",
                    "vlrenda": "vlrenda",
                    "pct_maxi_cpmm_rend": "pct_maxi_cpmm_rend",
                    "qtpzmax": "qtpzmax",
                    "vlrparcelaelegivelrefinanciamento": "vlrparcelaelegivelrefinanciamento"
                }
            ),
            
            # Visão de crédito do cliente
            "customer_credit_view": TableMapping(
                database="ricdb",
                table="tbfc6034_vsao_cred_clie",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "Ind_clie_corn_anti": "Ind_clie_corn_anti",
                    "cod_grup_vsao_clie": "cod_grup_vsao_clie",
                    "cod_crgo_clie": "cod_crgo_clie",
                    "cod_vncl_orgo_pubi": "cod_vncl_orgo_pubi"
                }
            ),
            
            # Cliente de visão de crédito
            "vsao_cred_clie": TableMapping(
                database="ricdb",
                table="tbfc6034_vsao_cred_clie",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "Ind_clie_corn_anti": "Ind_clie_corn_anti",
                    "cod_grup_vsao_clie": "cod_grup_vsao_clie",
                    "cod_crgo_clie": "cod_crgo_clie",
                    "cod_vncl_orgo_pubi": "cod_vncl_orgo_pubi"
                }
            ),
            
            # Dados manuais - grupo de visão do cliente
            "grup_vsao_clie_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6418_grup_vsao_clie_dm",
                column_mapping={}
            ),
            
            # Modo de elemento de pessoa física
            "mode_elto_pfis": TableMapping(
                database="dados_manuais",
                table="tbfc6419_mode_elto_pfis",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_mode_elto_pfis": "cod_mode_elto_pfis"
                }
            ),
            
            # Crédito de cliente pessoa física
            "cred_clie_pfis": TableMapping(
                database="dados_manuais",
                table="tbfc6420_cred_clie_pfis",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_cred_clie_pfis": "cod_cred_clie_pfis"
                }
            ),
            
            # Pagamento de salário SPI
            "pgto_salr_spi": TableMapping(
                database="dados_manuais",
                table="tbfc6421_pgto_salr_spi",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_pgto_salr_spi": "cod_pgto_salr_spi"
                }
            ),
            
            # Transferência de conta salário
            "trsf_cont_salr": TableMapping(
                database="dados_manuais",
                table="tbfc6422_trsf_cont_salr",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_trsf_cont_salr": "cod_trsf_cont_salr"
                }
            ),
            
            # Base de dados de modo de classificação Bradesco
            "dtbc_mode_claf_brau": TableMapping(
                database="dados_manuais",
                table="tbfc6430_dtbc_mode_claf_brau",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_dtbc_mode_claf_brau": "cod_dtbc_mode_claf_brau"
                }
            ),
            
            # Dados manuais - modo cruzado consignado
            "mode_csgd_crss_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6431_mode_csgd_crss_dm",
                column_mapping={
                    "ind_rati_csgd": "ind_rati_csgd"
                }
            ),
            
            # Dados manuais - cargo do cliente
            "crgo_clie_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6671_crgo_clie_dm",
                column_mapping={
                    "nom_crgo": "nom_crgo",
                    "nom_vncl": "nom_vncl"
                }
            ),
            
            # Dados manuais - vínculo órgão público
            "vncl_orgo_pubi_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6432_vncl_orgo_pubi_dm",
                column_mapping={
                    "num_ctrt_mae_lgdo": "cod_iden_conv_cred_csgd"
                }
            ),
            
            # Risco de crédito consignado
            "risc_cred_csgb": TableMapping(
                database="dados_manuais",
                table="tbfc6433_risc_cred_csgb",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_risc_cred_csgb": "cod_risc_cred_csgb"
                }
            ),
            
            # Contrato de crédito consignado
            "ctrt_cred_csgd": TableMapping(
                database="ricdb",
                table="tbfc6035_ctrt_cred_csgd",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "num_ctrt_cred_csgd": "num_ctrt_cred_csgd",
                    "vlr_ctrt_cred_csgd": "vlr_ctrt_cred_csgd",
                    "vlr_parc_cred_csgd": "vlr_parc_cred_csgd",
                    "qtd_parc_cred_csgd": "qtd_parc_cred_csgd",
                    "dat_venc_prim_parc": "dat_venc_prim_parc",
                    "dat_venc_ulti_parc": "dat_venc_ulti_parc",
                    "cod_iden_conv_cred_csgd": "cod_iden_conv_cred_csgd",
                    "cod_tipo_oper_cred": "cod_tipo_oper_cred",
                    "cod_situ_ctrt_cred": "cod_situ_ctrt_cred"
                }
            ),
            
            # Elemento de renda de pessoa física
            "rend_elto_pfis": TableMapping(
                database="ricdb",
                table="tbfc6036_rend_elto_pfis",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "vlr_rend_brut_mens": "vlr_rend_brut_mens",
                    "vlr_rend_liqu_mens": "vlr_rend_liqu_mens",
                    "cod_mode_elto_pfis": "cod_mode_elto_pfis",
                    "dat_admi_empr": "dat_admi_empr",
                    "dat_nasc": "dat_nasc"
                }
            ),
            
            # Contrato ativo
            "active_contract": TableMapping(
                database="ricdb",
                table="tbfc6037_cont_ativ",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_tipo_cont": "cod_tipo_cont",
                    "cod_cont_ativ": "cod_cont_ativ"
                },
                conditions=["cod_tipo_cont = 'C'", "cod_cont_ativ = 1"]
            ),
            
            # Classificação de emprego
            "employment_classification": TableMapping(
                database="ricdb",
                table="tbfc6512_claf_empr",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_claf_empr": "cod_claf_empr"
                }
            ),
            
            # Posição do cliente
            "customer_position": TableMapping(
                database="ricdb",
                table="tbfc6513_posi_clie",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_posi_clie": "cod_posi_clie"
                }
            ),
            
            # Risco de crédito atribuído
            "credit_risk_assigned": TableMapping(
                database="ricdb",
                table="tbfc6514_risc_cred_atri",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_risc_cred_atri": "cod_risc_cred_atri"
                }
            )
        }
        
        # Mapeamentos de chaves de junção para operações comuns
        self.join_keys = {
            "person_id": "cod_idef_pess",
            "convenio_id": "nrmaer",
            "contract_id": "cod_ctrt"
        }
        
        # Aliases de banco de dados para referência mais fácil
        self.database_aliases = {
            "ricdb": "ricdb",
            "consignado": "db_corp_servicosdecontratacao_consignado_sor_01",
            "keyspace": "keyspace_db",
            "dados_manuais": "dados_manuais"
        }
    
    def get_table_mapping(self, table_key: str) -> Optional[TableMapping]:
        """Obtém configuração de mapeamento de tabela por chave"""
        return self.table_mappings.get(table_key)
    
    def get_database_alias(self, alias: str) -> Optional[str]:
        """Obtém nome do banco de dados por alias"""
        return self.database_aliases.get(alias)
    
    def get_join_key(self, key_type: str) -> Optional[str]:
        """Obtém chave de junção por tipo"""
        return self.join_keys.get(key_type)


def get_mapped_query_parts(
    table_key: str, 
    select_fields: List[str], 
    where_conditions: Optional[List[str]] = None
) -> Tuple[str, str, str]:
    """
    Obtém partes da consulta (SELECT, FROM, WHERE) para uma tabela mapeada.
    
    Args:
        table_key: Chave para mapeamento de tabela
        select_fields: Lista de campos para selecionar
        where_conditions: Condições WHERE opcionais
        
    Returns:
        Tupla de (cláusula SELECT, cláusula FROM, cláusula WHERE)
    """
    db_config = DatabaseMappingConfig()
    mapping = db_config.get_table_mapping(table_key)
    
    if not mapping:
        raise ValueError(f"Mapeamento de tabela não encontrado para a chave: {table_key}")
    
    # Constrói cláusula SELECT
    if select_fields == ["*"]:
        select_clause = "SELECT *"
    else:
        # Mapeia nomes de campos se o mapeamento de colunas existir
        mapped_fields = []
        for field in select_fields:
            mapped_field = mapping.column_mapping.get(field, field)
            mapped_fields.append(mapped_field)
        select_clause = f"SELECT {', '.join(mapped_fields)}"
    
    # Constrói cláusula FROM
    from_clause = f"FROM {mapping.database}.{mapping.table}"
    
    # Constrói cláusula WHERE
    where_parts = []
    if mapping.conditions:
        where_parts.extend(mapping.conditions)
    if where_conditions:
        where_parts.extend(where_conditions)
    
    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    
    return select_clause, from_clause, where_clause


def build_standardized_query(
    table_key: str,
    select_fields: List[str],
    where_conditions: Optional[List[str]] = None,
    partition_filter: Optional[str] = None
) -> str:
    """
    Constrói uma consulta SQL padronizada para uma tabela mapeada.
    
    Args:
        table_key: Chave para mapeamento de tabela
        select_fields: Lista de campos para selecionar
        where_conditions: Condições WHERE opcionais
        partition_filter: Filtro de partição opcional
        
    Returns:
        String de consulta SQL completa
    """
    select_clause, from_clause, where_clause = get_mapped_query_parts(
        table_key, select_fields, where_conditions
    )
    
    # Adiciona filtro de partição se fornecido
    if partition_filter:
        if where_clause:
            where_clause += f" AND {partition_filter}"
        else:
            where_clause = f"WHERE {partition_filter}"
    
    # Combina todas as partes
    query_parts = [select_clause, from_clause]
    if where_clause:
        query_parts.append(where_clause)
    
    return " ".join(query_parts)


# Instância global para acesso fácil
db_mapping = DatabaseMappingConfig()
