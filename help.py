"""
Database Mapping Configuration Module

This module centralizes all database table and column mappings based on the
listaReferencia.txt file. It provides a standardized way to access database
relationships and column mappings across the project.

Author: Data Engineering Team
"""

from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class TableMapping:
    """Represents a database table mapping with its full path and column mappings."""
    database: str
    table: str
    columns: Dict[str, str]  # original_column -> mapped_column
    conditions: Optional[Dict[str, str]] = None  # column -> condition_value


class DatabaseMappingConfig:
    """Central configuration for database mappings and relationships."""
    
    def __init__(self):
        self._initialize_mappings()
    
    def _initialize_mappings(self):
        """Initialize all database mappings based on listaReferencia.txt."""
        
        # Core database mappings from listaReferencia.txt
        self.table_mappings = {
            # Convenio and collaboration info
            "conv_cola_info": TableMapping(
                database="db_corp_servicosdecontratacao_dataplataformpf_sec_01",
                table="tbln9_rlmt_conv_cola_info_minm",
                columns={"cod_matr_cola": "cod_matr_cola"}
            ),
            
            # Employee contract info
            "employee_contract_info": TableMapping(
                database="db_corp_servicosdecontratacao_dataplataformpf_sec_01",
                table="tbln9_rlmt_conv_cola_info_minm",
                columns={"cod_matr_cola": "cod_matr_cola"}
            ),
            
            # Aurora query core tables
            "customer_credit_view": TableMapping(
                database="ricdb",
                table="tbfc6034_vsao_cred_clie",
                columns={"cod_idef_pess": "cod_idef_pess"}
            ),
            "customer_model_element": TableMapping(
                database="ricdb",
                table="tbfc6230_mode_elto_pf",
                columns={"cod_idef_pess": "cod_idef_pess"}
            ),
            "credit_risk_central_bank": TableMapping(
                database="ricdb",
                table="tbfc6177_risc_cred_banc_cenl",
                columns={"cod_idef_pess": "cod_idef_pess"}
            ),
            
            # Credit client vision
            "vsao_cred_clie": TableMapping(
                database="ricdb",
                table="tbfc6034_vsao_cred_clie",
                columns={"ind_inle_frte_clie": "ind_inle_frte_clie"}
            ),
            
            # Credit contract consigned - with conditions
            "ctrt_cred_csgd": TableMapping(
                database="ricdb",
                table="tbfc6_ctrt_cred_csgd",
                columns={
                    "vlr_tol_cntr": "vlr_tol_cntr",
                    "vlr_totl_parc_oper": "vlr_totl_parc_oper"
                },
                conditions={
                    "vlr_tol_cntr": "COD_PROD_CRED_APRE_APRA = '027'",
                    "vlr_totl_parc_oper": "COD_PROD_CRED_APRE_APRA IN ('027', '030')"
                }
            ),
            
            # Physical person income
            "rend_elto_pfis": TableMapping(
                database="ricdb",
                table="tbfc6547_rend_elto_pfis",
                columns={}
            ),
            
            # Manual data - client vision group
            "grup_vsao_clie_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6418_grup_vsao_clie_dm",
                columns={}
            ),
            
            # Physical person mode
            "mode_elto_pfis": TableMapping(
                database="ricdb",
                table="tbfc6230_mode_elto_pfis",
                columns={"cod_mode_elto": "cod_mode_elto"}
            ),
            
            # Physical person credit client
            "cred_clie_pfis": TableMapping(
                database="ricdb",
                table="tbfc6037_cred_clie_pfis",
                columns={"dat_nasc_pfis": "dat_nasc_pfis"}
            ),
            
            # SPI salary payment
            "pgto_salr_spi": TableMapping(
                database="ricdb",
                table="tbfc6682_pgto_salr_spi",
                columns={
                    "spi": "spi",
                    "vlr_lanc_cred_favo": "vlr_lanc_cred_favo"
                }
            ),
            
            # Salary account transfer
            "trsf_cont_salr": TableMapping(
                database="ricdb",
                table="tbfc6505_trsf_cont_salr",
                columns={}
            ),
            
            # Employment gene classification
            "dtbc_mode_claf_brau": TableMapping(
                database="db_corp_modelagem_distribuicaomodelo_spec_01",
                table="tbmd7023_dtbc_mode_claf_brau",
                columns={"id_gene_emprego": "id_gene_emprego"}
            ),
            
            # Manual data - consigned mode cross
            "mode_csgd_crss_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6431_mode_csgd_crss_dm",
                columns={"ind_rati_csgd": "ind_rati_csgd"}
            ),
            
            # Manual data - client position
            "crgo_clie_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6671_crgo_clie_dm",
                columns={
                    "nom_crgo": "nom_crgo",
                    "nom_vncl": "nom_vncl"
                }
            ),
            
            # Manual data - public organ link
            "vncl_orgo_pubi_dm": TableMapping(
                database="dados_manuais",
                table="tbfc6432_vncl_orgo_pubi_dm",
                columns={"num_ctrt_mae_lgdo": "cod_iden_conv_cred_csgd"}
            ),
            
            # Credit risk consigned
            "risc_cred_csgb": TableMapping(
                database="ricdb",
                table="tbfc6646_risc_cred_csgb",
                columns={"elegivel_refin": "elegivel_refin"}
            ),
            
            # Convention management events
            "events_convenio": TableMapping(
                database="db_corp_emprestimosefinanciamentos_icaasconvecios_sor_01",
                table="tbxc2_events_emprestimos_e_financiamentos_gestao_de_convenio_parametrizacao_atualizado",
                columns={"cod_convenio": "cod_convenio"}
            ),
            
            # Credit margin
            "marg_cred": TableMapping(
                database="db_corp_emprestimosefinanciamentos_icaascontratacao_sor_01",
                table="tbjx9105_marg_cred",
                columns={}
            ),
            
            # Additional Aurora query tables
            "customer_registration_notes": TableMapping(
                database="ricdb",
                table="tbfc6036_apon_cadl_clie",
                columns={"cod_idef_pess": "cod_idef_pess"}
            ),
            "customer_credit_individual": TableMapping(
                database="ricdb",
                table="tbfc6037_cred_clie_pfis",
                columns={"cod_idef_pess": "cod_idef_pess"}
            ),
            "salary_account_transfer": TableMapping(
                database="ricdb",
                table="tbfc6505_trsf_cont_salr",
                columns={"cod_idef_pess": "cod_idef_pess"}
            ),
            
            # Keyspace query tables
            "public_person_view": TableMapping(
                database="ricdb",
                table="tbfc66706_pubi_vsao_pess",
                columns={"cod_idef_pess": "cod_idef_pess"}
            ),
            "employment_classification": TableMapping(
                database="ricdb",
                table="tbmd7023_dtbc_mode_claf_brau",
                columns={"cod_chav_coro_clie": "cod_chav_coro_clie"}
            ),
            "customer_position": TableMapping(
                database="dados_manuais",
                table="tbfc6671_crgo_clie_dm",
                columns={"cod_chav_coro_clie": "cod_chav_coro_clie"}
            ),
            "credit_risk_assigned": TableMapping(
                database="ricdb",
                table="tbfc6646_risc_cred_csgb",
                columns={"cod_chav_coro_clie": "cod_chav_coro_clie"}
            )
        }
        
        # Common join keys mapping
        self.join_keys = {
            "client_person": "cod_idef_pess",
            "client_account": "cod_chav_coro_clie", 
            "convention": "nrmaer",
            "agency_account": ["cdagecdt", "cdctacdt"],
            "convention_contract": "cod_iden_conv_cred_csgd"
        }
        
        # Database aliases for easier reference
        self.database_aliases = {
            "ricdb": "ricdb",
            "dados_manuais": "dados_manuais",
            "manual_data": "dados_manuais",
            "corp_services": "db_corp_servicosdecontratacao_dataplataformpf_sec_01",
            "corp_consigned": "db_corp_servicosdecontratacao_consignado_sor_01",
            "corp_modeling": "db_corp_modelagem_distribuicaomodelo_spec_01",
            "corp_loans_conventions": "db_corp_emprestimosefinanciamentos_icaasconvecios_sor_01",
            "corp_loans_contracting": "db_corp_emprestimosefinanciamentos_icaascontratacao_sor_01",
            "db_corp_servicosdecontratacao_dataplataformpf_sec_01": "corp_services_db"
        }
    
    def get_table_mapping(self, table_key: str) -> Optional[TableMapping]:
        """Get table mapping by key."""
        return self.table_mappings.get(table_key)
    
    def get_database_alias(self, database_name: str) -> str:
        """Get database alias for a given database name."""
        return self.database_aliases.get(database_name, database_name)
    
    def get_full_table_name(self, table_key: str) -> Optional[str]:
        """Get full table name (database.table) by key."""
        mapping = self.get_table_mapping(table_key)
        if mapping:
            return f"{mapping.database}.{mapping.table}"
        return None
    
    def get_column_mapping(self, table_key: str, column: str) -> Optional[str]:
        """Get mapped column name for a specific table and column."""
        mapping = self.get_table_mapping(table_key)
        if mapping and column in mapping.columns:
            return mapping.columns[column]
        return column  # Return original if no mapping found
    
    def get_table_conditions(self, table_key: str) -> Optional[Dict[str, str]]:
        """Get conditions for a specific table."""
        mapping = self.get_table_mapping(table_key)
        if mapping:
            return mapping.conditions
        return None
    
    def get_join_key(self, join_type: str) -> Optional[str]:
        """Get join key for a specific join type."""
        return self.join_keys.get(join_type)
    
    def get_database_by_alias(self, alias: str) -> Optional[str]:
        """Get database name by alias."""
        return self.database_aliases.get(alias)
    
    def list_available_tables(self) -> List[str]:
        """List all available table keys."""
        return list(self.table_mappings.keys())
    
    def validate_table_mapping(self, table_key: str) -> bool:
        """Validate if a table mapping exists and is properly configured."""
        mapping = self.get_table_mapping(table_key)
        if not mapping:
            return False
        
        # Check if database and table are properly set
        return bool(mapping.database and mapping.table)


# Global instance for easy access
db_mapping = DatabaseMappingConfig()


def get_mapped_query_parts(table_key: str, select_fields: List[str], 
                          where_conditions: Optional[List[str]] = None) -> Dict[str, any]:
    """
    Get mapped query parts for a given table key.
    
    Args:
        table_key: Key to identify the table mapping
        select_fields: List of fields to select
        where_conditions: Optional list of WHERE conditions
        
    Returns:
        Dictionary with database, table, select_fields, and where_conditions
    """
    mapping = db_mapping.get_table_mapping(table_key)
    if not mapping:
        raise ValueError(f"No mapping found for table key: {table_key}")
    
    # Map select fields
    mapped_fields = []
    for field in select_fields:
        mapped_field = mapping.columns.get(field, field)
        mapped_fields.append(mapped_field)
    
    # Map where conditions if provided
    mapped_conditions = []
    if where_conditions:
        for condition in where_conditions:
            # Simple mapping - replace column names in conditions
            mapped_condition = condition
            for original_col, mapped_col in mapping.columns.items():
                mapped_condition = mapped_condition.replace(original_col, mapped_col)
            mapped_conditions.append(mapped_condition)
    
    # Add table-specific conditions if any
    if mapping.conditions:
        for column, value in mapping.conditions.items():
            mapped_column = mapping.columns.get(column, column)
            mapped_conditions.append(f"{mapped_column} = '{value}'")
    
    return {
        'database': mapping.database,
        'table': mapping.table,
        'select_fields': mapped_fields,
        'where_conditions': mapped_conditions
    }


def build_standardized_query(table_key: str, select_fields: List[str], 
                           where_conditions: Optional[List[str]] = None) -> str:
    """
    Build a complete standardized SQL query based on mapping configuration.
    
    Args:
        table_key: Key to identify the table mapping
        select_fields: List of fields to select with their aliases
        where_conditions: Optional list of WHERE conditions
    
    Returns:
        Complete SQL query string
    """
    query_parts = get_mapped_query_parts(
        table_key, select_fields, where_conditions
    )
    
    full_table_name = f"{query_parts['database']}.{query_parts['table']}"
    select_clause = ", ".join(query_parts['select_fields'])
    
    query = f"SELECT {select_clause} FROM {full_table_name}"
    if query_parts['where_conditions']:
        where_clause = " AND ".join(query_parts['where_conditions'])
        query += f" WHERE {where_clause}"
    
    return query
