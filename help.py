"""
Database Mapping Configuration Module

This module provides a centralized configuration system for database table mappings,
column specifications, and query building utilities for the credit analysis ETL process.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class TableMapping:
    """Represents a database table mapping with column specifications"""
    database: str
    table: str
    column_mapping: Dict[str, str]
    conditions: Optional[List[str]] = None


class DatabaseMappingConfig:
    """Central configuration for database table mappings and query building"""
    
    def __init__(self):
        """Initialize database mappings for all required tables"""
        self.table_mappings = {
            # Convenio and collaboration info tables
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
                    "ind_inle_frte_clie": "ind_inle_frte_clie"
                }
            ),
            
            # Customer credit individual
            "vsao_cred_clie": TableMapping(
                database="ricdb", 
                table="tbfc6034_vsao_cred_clie",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "Ind_clie_corn_anti": "Ind_clie_corn_anti", 
                    "ind_inle_frte_clie": "ind_inle_frte_clie"
                }
            ),
            
            # Credit contract consigned
            "ctrt_cred_csgd": TableMapping(
                database="ricdb",
                table="tbfc6035_ctrt_cred_csgd",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "vlr_ctrt_cred": "vlr_ctrt_cred"
                }
            ),
            
            # Income element physical person
            "rend_elto_pfis": TableMapping(
                database="ricdb",
                table="tbfc6038_rend_elto_pfis", 
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "vlr_rend_elto": "vlr_rend_elto"
                }
            ),
            
            # Manual data - client vision group
            "grup_vsao_clie_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6418_grup_vsao_clie_dm",
                column_mapping={}
            ),
            
            # Model element physical person
            "mode_elto_pfis": TableMapping(
                database="ricdb",
                table="tbfc6230_mode_elto_pf",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_mode_elto": "cod_mode_elto"
                }
            ),
            
            # Customer credit physical person
            "cred_clie_pfis": TableMapping(
                database="ricdb",
                table="tbfc6037_cred_clie_pfis",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "dat_nasc_pfis": "dat_nasc_pfis"
                }
            ),
            
            # Salary payment SPI
            "pgto_salr_spi": TableMapping(
                database="ricdb",
                table="tbfc6504_pgto_salr_spi",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_mens_tran": "cod_mens_tran"
                }
            ),
            
            # Salary account transfer
            "trsf_cont_salr": TableMapping(
                database="ricdb", 
                table="tbfc6505_trsf_cont_salr",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_mens_tran": "cod_mens_tran"
                }
            ),
            
            # Database model classification BRAU
            "dtbc_mode_claf_brau": TableMapping(
                database="ricdb",
                table="tbfc6506_dtbc_mode_claf_brau",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_mode_claf": "cod_mode_claf"
                }
            ),
            
            # Manual data - consigned mode cross 
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
            
            # Credit risk central bank
            "risc_cred_csgb": TableMapping(
                database="ricdb",
                table="tbfc6177_risc_cred_banc_cenl",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_moda_sbme_oper": "cod_moda_sbme_oper",
                    "vlr_cred_vncr_30": "vlr_cred_vncr_30",
                    "cod_inst_finn_cogl": "cod_inst_finn_cogl",
                    "cod_tipo_pess": "cod_tipo_pess"
                },
                conditions=["cod_tipo_pess = 'F'"]
            ),
            
            # Events convenio
            "events_convenio": TableMapping(
                database="ricdb",
                table="tbfc6510_even_conv",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_even_conv": "cod_even_conv"
                }
            ),
            
            # Credit margin
            "marg_cred": TableMapping(
                database="ricdb",
                table="tbfc6511_marg_cred",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "vlr_marg_cred": "vlr_marg_cred"
                }
            ),
            
            # Customer registration notes
            "customer_registration_notes": TableMapping(
                database="ricdb",
                table="tbfc6036_apon_cadl_clie",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "dat_apon_cadl_clie": "dat_apon_cadl_clie",
                    "cod_tipo_apon_cadl": "cod_tipo_apon_cadl"
                }
            ),
            
            # Customer credit individual
            "customer_credit_individual": TableMapping(
                database="ricdb",
                table="tbfc6037_cred_clie_pfis",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "dat_nasc_pfis": "dat_nasc_pfis"
                }
            ),
            
            # Salary account transfer
            "salary_account_transfer": TableMapping(
                database="ricdb",
                table="tbfc6505_trsf_cont_salr",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_mens_tran": "cod_mens_tran"
                }
            ),
            
            # Public person view
            "public_person_view": TableMapping(
                database="ricdb",
                table="tbfc66706_pubi_vsao_pess",
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
                    "cod_risc_cred": "cod_risc_cred"
                }
            ),
            
            # Customer model element
            "customer_model_element": TableMapping(
                database="ricdb",
                table="tbfc6230_mode_elto_pf",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_mode_elto": "cod_mode_elto"
                }
            ),
            
            # Credit risk central bank
            "credit_risk_central_bank": TableMapping(
                database="ricdb",
                table="tbfc6177_risc_cred_banc_cenl",
                column_mapping={
                    "cod_idef_pess": "cod_idef_pess",
                    "cod_moda_sbme_oper": "cod_moda_sbme_oper",
                    "vlr_cred_vncr_30": "vlr_cred_vncr_30",
                    "cod_inst_finn_cogl": "cod_inst_finn_cogl",
                    "cod_tipo_pess": "cod_tipo_pess"
                },
                conditions=["cod_tipo_pess = 'F'"]
            )
        }
        
        # Common join keys for table relationships
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
    Build query parts using database mapping configuration.
    
    Args:
        table_key: Key to identify table in mapping configuration
        select_fields: List of fields to select
        where_conditions: Optional list of WHERE conditions
    
    Returns:
        Tuple of (full_table_name, select_clause, where_clause)
    """
    db_mapping = DatabaseMappingConfig()
    table_mapping = db_mapping.get_table_mapping(table_key)
    
    if not table_mapping:
        raise ValueError(f"Table mapping not found for key: {table_key}")
    
    # Build full table name
    full_table_name = f"{table_mapping.database}.{table_mapping.table}"
    
    # Build select clause
    select_clause = ", ".join(select_fields)
    
    # Build where clause
    where_parts = []
    
    # Add table-specific conditions
    if table_mapping.conditions:
        where_parts.extend(table_mapping.conditions)
    
    # Add custom conditions
    if where_conditions:
        where_parts.extend(where_conditions)
    
    where_clause = " AND ".join(where_parts) if where_parts else ""
    
    return full_table_name, select_clause, where_clause


def build_standardized_query(
    table_key: str,
    select_fields: List[str],
    where_conditions: Optional[List[str]] = None,
    partition_filter: Optional[str] = None
) -> str:
    """
    Build a standardized SQL query using database mapping configuration.
    
    Args:
        table_key: Key to identify table in mapping configuration
        select_fields: List of fields to select
        where_conditions: Optional list of WHERE conditions
        partition_filter: Optional partition filter
    
    Returns:
        Complete SQL query string
    """
    full_table_name, select_clause, where_clause = get_mapped_query_parts(
        table_key, select_fields, where_conditions
    )
    
    query = f"SELECT {select_clause} FROM {full_table_name}"
    
    # Build complete WHERE clause
    where_parts = []
    
    if partition_filter:
        where_parts.append(f"({partition_filter})")
    
    if where_clause:
        where_parts.append(f"({where_clause})")
    
    if where_parts:
        query += f" WHERE {' AND '.join(where_parts)}"
    
    return query


# Global instance for easy access
db_mapping = DatabaseMappingConfig()
