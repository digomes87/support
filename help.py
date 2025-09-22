"""
JSON Schema Validator for ETL Output Format
Validates that the generated JSON matches the expected structure for credit analysis.
"""

import json
from typing import Dict, Any, List, Optional
from utils.logging_config import DataValidationError, ETLLogger


class JSONSchemaValidator:
    """Validates JSON output against expected schema for credit analysis."""
    
    def __init__(self, logger: ETLLogger):
        self.logger = logger
        self.expected_schema = self._define_expected_schema()
    
    def _define_expected_schema(self) -> Dict[str, Any]:
        """Define the expected JSON schema structure."""
        return {
            "payloadIdentification": {
                "required_fields": [
                    "nom_fncd_serv_nego",
                    "cod_idef_prpt_sis_prod", 
                    "cod_idef_tran_sist_cred",
                    "cod_idef_job_jorn",
                    "cod_idef_tran_sis_prod",
                    "cod_idef_prso_nego",
                    "cod_idef_pess",
                    "nom_prso_sist_cred",
                    "cod_stat_desc",
                    "nom_tipo_desc_prpt",
                    "nom_moto_anal_prpt",
                    "cod_tipo_pess",
                    "nom_prod_cred",
                    "nom_fase_cred",
                    "nom_pilo_pltc_cred",
                    "cod_vers_pltc",
                    "cod_vers_tecn_pltc",
                    "dat_hor_exeo_decs"
                ],
                "field_types": {
                    "nom_fncd_serv_nego": str,
                    "cod_idef_prso_nego": str,
                    "cod_tipo_pess": str
                }
            },
            "payloadInput": {
                "required_fields": [
                    "tipoProcesso",
                    "codigoProdutoOperacao", 
                    "Matricula",
                    "codigoRDG"
                ],
                "field_types": {
                    "tipoProcesso": str
                }
            },
            "dadosOferta": {
                "required_fields": ["flagInelegibilidadeForte"],
                "field_types": {
                    "flagInelegibilidadeForte": bool
                }
            },
            "digitoAleatorio": {
                "type": int,
                "range": [0, 100]
            },
            "listaContratos": {
                "type": list,
                "item_schema": {
                    "required_fields": ["valorContrato"],
                    "field_types": {
                        "valorContrato": (int, float)
                    }
                }
            },
            "dadosEndividamento": {
                "required_fields": [
                    "valorParcConsignado",
                    "valorParcConsignadoItau"
                ],
                "field_types": {
                    "valorParcConsignado": (int, float),
                    "valorParcConsignadoItau": (int, float)
                }
            },
            "dadosFatorRisco": {
                "required_fields": ["listaApontamento", "listaExclusao"],
                "field_types": {
                    "listaApontamento": list,
                    "listaExclusao": list
                },
                "nested_schemas": {
                    "listaApontamento": {
                        "required_fields": ["codigo", "recencia"],
                        "field_types": {
                            "codigo": int,
                            "recencia": int
                        }
                    },
                    "listaExclusao": {
                        "required_fields": ["codigo"],
                        "field_types": {
                            "codigo": int
                        }
                    }
                }
            }
        }
    
    def validate_json_structure(self, json_data: Dict[str, Any]) -> bool:
        """
        Validate JSON structure against expected schema.
        
        Args:
            json_data: The JSON data to validate
            
        Returns:
            bool: True if valid, raises DataValidationError if invalid
        """
        try:
            self.logger.log_operation_start("JSON Schema Validation")
            
            # Validate top-level structure
            self._validate_top_level_structure(json_data)
            
            # Validate each section
            for section_name, section_schema in self.expected_schema.items():
                if section_name in json_data:
                    self._validate_section(section_name, json_data[section_name], section_schema)
                else:
                    self.logger.logger.warning(f"Missing optional section: {section_name}")
            
            self.logger.log_operation_end("JSON Schema Validation")
            return True
            
        except Exception as e:
            raise DataValidationError(f"JSON schema validation failed: {str(e)}", 
                                    "json_validation", 
                                    {"error": str(e)})
    
    def _validate_top_level_structure(self, json_data: Dict[str, Any]) -> None:
        """Validate the top-level structure of the JSON."""
        required_top_level = ["payloadIdentification", "payloadInput"]
        
        for field in required_top_level:
            if field not in json_data:
                raise ValueError(f"Missing required top-level field: {field}")
    
    def _validate_section(self, section_name: str, section_data: Any, section_schema: Dict[str, Any]) -> None:
        """Validate a specific section of the JSON."""
        if section_name == "digitoAleatorio":
            self._validate_simple_field(section_name, section_data, section_schema)
            return
        
        if not isinstance(section_data, dict) and section_name != "listaContratos":
            raise ValueError(f"Section {section_name} must be an object")
        
        # Validate required fields
        if "required_fields" in section_schema:
            for field in section_schema["required_fields"]:
                if field not in section_data:
                    raise ValueError(f"Missing required field '{field}' in section '{section_name}'")
        
        # Validate field types
        if "field_types" in section_schema:
            for field, expected_type in section_schema["field_types"].items():
                if field in section_data:
                    self._validate_field_type(f"{section_name}.{field}", section_data[field], expected_type)
        
        # Validate nested schemas
        if "nested_schemas" in section_schema:
            for field, nested_schema in section_schema["nested_schemas"].items():
                if field in section_data and isinstance(section_data[field], list):
                    for i, item in enumerate(section_data[field]):
                        self._validate_nested_item(f"{section_name}.{field}[{i}]", item, nested_schema)
    
    def _validate_simple_field(self, field_name: str, value: Any, schema: Dict[str, Any]) -> None:
        """Validate a simple field against its schema."""
        expected_type = schema.get("type")
        if expected_type and not isinstance(value, expected_type):
            raise ValueError(f"Field '{field_name}' must be of type {expected_type.__name__}")
        
        if "range" in schema and isinstance(value, (int, float)):
            min_val, max_val = schema["range"]
            if not (min_val <= value <= max_val):
                raise ValueError(f"Field '{field_name}' must be between {min_val} and {max_val}")
    
    def _validate_field_type(self, field_path: str, value: Any, expected_type: Any) -> None:
        """Validate that a field has the expected type."""
        if isinstance(expected_type, tuple):
            if not isinstance(value, expected_type):
                type_names = " or ".join([t.__name__ for t in expected_type])
                raise ValueError(f"Field '{field_path}' must be of type {type_names}")
        else:
            if not isinstance(value, expected_type):
                raise ValueError(f"Field '{field_path}' must be of type {expected_type.__name__}")
    
    def _validate_nested_item(self, item_path: str, item: Dict[str, Any], schema: Dict[str, Any]) -> None:
        """Validate a nested item against its schema."""
        if not isinstance(item, dict):
            raise ValueError(f"Item at '{item_path}' must be an object")
        
        # Validate required fields
        for field in schema.get("required_fields", []):
            if field not in item:
                raise ValueError(f"Missing required field '{field}' in item at '{item_path}'")
        
        # Validate field types
        for field, expected_type in schema.get("field_types", {}).items():
            if field in item:
                self._validate_field_type(f"{item_path}.{field}", item[field], expected_type)
    
    def validate_json_file(self, file_path: str) -> bool:
        """
        Validate a JSON file against the expected schema.
        
        Args:
            file_path: Path to the JSON file to validate
            
        Returns:
            bool: True if valid, raises DataValidationError if invalid
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            return self.validate_json_structure(json_data)
            
        except json.JSONDecodeError as e:
            raise DataValidationError(f"Invalid JSON format in file {file_path}: {str(e)}", 
                                    "json_parsing", 
                                    {"file": file_path, "error": str(e)})
        except FileNotFoundError:
            raise DataValidationError(f"JSON file not found: {file_path}", 
                                    "file_not_found", 
                                    {"file": file_path})


def validate_etl_output_format(json_data: Dict[str, Any], logger: ETLLogger) -> bool:
    """
    Convenience function to validate ETL output format.
    
    Args:
        json_data: The JSON data to validate
        logger: ETL logger instance
        
    Returns:
        bool: True if valid, raises DataValidationError if invalid
    """
    validator = JSONSchemaValidator(logger)
    return validator.validate_json_structure(json_data)
