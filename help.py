"""
Database mapping configuration for ETL operations.
Centralizes all database table mappings, column specifications, and query utilities.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class TableMapping:
    """Configuration for database table mapping."""
    database: str
    table: str
    column_mapping: Dict[str, str]
    conditions: Optional[List[str]] = None


class DatabaseMappingConfig:
    """Central configuration for all database mappings and queries."""
    
    def __init__(self):
        """Initialize database mappings and configurations."""
        self.table_mappings = {
            # Convenio collaboration info
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
            
            # Employee contract info
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
            
            # Customer credit view
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
            
            # Credit view client
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
            
            # Manual data - client vision group
            "grup_vsao_clie_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6418_grup_vsao_clie_dm",
                column_mapping={}
            ),
            
            # Physical person element mode
            "mode_elto_pfis": TableMapping(
                database="dados_manuais",
                table="tbfc6419_mode_elto_pfis",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_mode_elto_pfis": "cod_mode_elto_pfis"
                }
            ),
            
            # Physical person credit
            "cred_clie_pfis": TableMapping(
                database="dados_manuais",
                table="tbfc6420_cred_clie_pfis",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_cred_clie_pfis": "cod_cred_clie_pfis"
                }
            ),
            
            # SPI salary payment
            "pgto_salr_spi": TableMapping(
                database="dados_manuais",
                table="tbfc6421_pgto_salr_spi",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_pgto_salr_spi": "cod_pgto_salr_spi"
                }
            ),
            
            # Salary account transfer
            "trsf_cont_salr": TableMapping(
                database="dados_manuais",
                table="tbfc6422_trsf_cont_salr",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_trsf_cont_salr": "cod_trsf_cont_salr"
                }
            ),
            
            # Bradesco classification mode database
            "dtbc_mode_claf_brau": TableMapping(
                database="dados_manuais",
                table="tbfc6430_dtbc_mode_claf_brau",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_dtbc_mode_claf_brau": "cod_dtbc_mode_claf_brau"
                }
            ),
            
            # Manual data - consignado cross mode
            "mode_csgd_crss_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6431_mode_csgd_crss_dm",
                column_mapping={
                    "ind_rati_csgd": "ind_rati_csgd"
                }
            ),
            
            # Manual data - client position
            "crgo_clie_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6671_crgo_clie_dm",
                column_mapping={
                    "nom_crgo": "nom_crgo",
                    "nom_vncl": "nom_vncl"
                }
            ),
            
            # Manual data - public organ link
            "vncl_orgo_pubi_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6432_vncl_orgo_pubi_dm",
                column_mapping={
                    "num_ctrt_mae_lgdo": "cod_iden_conv_cred_csgd"
                }
            ),
            
            # Consignado credit risk
            "risc_cred_csgb": TableMapping(
                database="dados_manuais",
                table="tbfc6433_risc_cred_csgb",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_risc_cred_csgb": "cod_risc_cred_csgb"
                }
            ),
            
            # Consignado credit contract
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
            
            # Physical person income element
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
            
            # Active contract
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
            
            # Employment classification
            "employment_classification": TableMapping(
                database="ricdb",
                table="tbfc6512_claf_empr",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_claf_empr": "cod_claf_empr"
                }
            ),
            
            # Customer position
            "customer_position": TableMapping(
                database="ricdb",
                table="tbfc6513_posi_clie",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_posi_clie": "cod_posi_clie"
                }
            ),
            
            # Credit risk assigned
            "credit_risk_assigned": TableMapping(
                database="ricdb",
                table="tbfc6514_risc_cred_atri",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_risc_cred_atri": "cod_risc_cred_atri"
                }
            )
        }
        
        # Join key mappings for common operations
        self.join_keys = {
            "person_id": "cod_idef_pess",
            "convenio_id": "nrmaer",
            "contract_id": "cod_ctrt"
        }
        
        # Database aliases for easier reference
        self.database_aliases = {
            "ricdb": "ricdb",
            "consignado": "db_corp_servicosdecontratacao_consignado_sor_01",
            "keyspace": "keyspace_db",
            "dados_manuais": "dados_manuais"
        }
    
    def get_table_mapping(self, table_key: str) -> Optional[TableMapping]:
        """Get table mapping configuration by key"""
        return self.table_mappings.get(table_key)
    
    def get_database_alias(self, alias: str) -> Optional[str]:
        """Get database name by alias"""
        return self.database_aliases.get(alias)
    
    def get_join_key(self, key_type: str) -> Optional[str]:
        """Get join key by type"""
        return self.join_keys.get(key_type)


def get_mapped_query_parts(
    table_key: str, 
    select_fields: List[str], 
    where_conditions: Optional[List[str]] = None
) -> Tuple[str, str, str]:
    """
    Get query parts (SELECT, FROM, WHERE) for a mapped table.
    
    Args:
        table_key: Key for table mapping
        select_fields: List of fields to select
        where_conditions: Optional WHERE conditions
        
    Returns:
        Tuple of (SELECT clause, FROM clause, WHERE clause)
    """
    db_config = DatabaseMappingConfig()
    mapping = db_config.get_table_mapping(table_key)
    
    if not mapping:
        raise ValueError(f"Table mapping not found for key: {table_key}")
    
    # Build SELECT clause
    if select_fields == ["*"]:
        select_clause = "SELECT *"
    else:
        # Map field names if column mapping exists
        mapped_fields = []
        for field in select_fields:
            mapped_field = mapping.column_mapping.get(field, field)
            mapped_fields.append(mapped_field)
        select_clause = f"SELECT {', '.join(mapped_fields)}"
    
    # Build FROM clause
    from_clause = f"FROM {mapping.database}.{mapping.table}"
    
    # Build WHERE clause
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
    Build a standardized SQL query for a mapped table.
    
    Args:
        table_key: Key for table mapping
        select_fields: List of fields to select
        where_conditions: Optional WHERE conditions
        partition_filter: Optional partition filter
        
    Returns:
        Complete SQL query string
    """
    select_clause, from_clause, where_clause = get_mapped_query_parts(
        table_key, select_fields, where_conditions
    )
    
    # Add partition filter if provided
    if partition_filter:
        if where_clause:
            where_clause += f" AND {partition_filter}"
        else:
            where_clause = f"WHERE {partition_filter}"
    
    # Combine all parts
    query_parts = [select_clause, from_clause]
    if where_clause:
        query_parts.append(where_clause)
    
    return " ".join(query_parts)


# Global instance for easy access
db_mapping = DatabaseMappingConfig()
