"""
Centralized logging configuration for the ETL pipeline.
Provides consistent logging setup and error handling utilities.
"""

import logging
import sys
from typing import Optional


class ETLLogger:
    """Enhanced logger for ETL operations with structured logging."""
    
    def __init__(self, name: str, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Avoid duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()
    
    def _setup_handlers(self):
        """Setup console and file handlers with proper formatting."""
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler for errors
        error_handler = logging.FileHandler('etl_errors.log')
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        self.logger.addHandler(error_handler)
    
    def log_operation_start(self, operation: str, **kwargs):
        """Log the start of an ETL operation with context."""
        context = ', '.join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.info(f"Starting {operation} - {context}")
    
    def log_operation_end(self, operation: str, duration: Optional[float] = None):
        """Log the completion of an ETL operation."""
        duration_str = f" (Duration: {duration:.2f}s)" if duration else ""
        self.logger.info(f"Completed {operation}{duration_str}")
    
    def log_data_quality(self, stage: str, record_count: int, **metrics):
        """Log data quality metrics."""
        metrics_str = ', '.join([f"{k}={v}" for k, v in metrics.items()])
        self.logger.info(f"Data Quality - {stage}: {record_count} records, {metrics_str}")
    
    def log_error_with_context(self, error: Exception, operation: str, **context):
        """Log errors with detailed context for debugging."""
        context_str = ', '.join([f"{k}={v}" for k, v in context.items()])
        self.logger.error(
            f"Error in {operation}: {str(error)} - Context: {context_str}",
            exc_info=True
        )
    
    def log_aws_operation(self, service: str, operation: str, status: str, **details):
        """Log AWS service operations."""
        details_str = ', '.join([f"{k}={v}" for k, v in details.items()])
        self.logger.info(f"AWS {service} - {operation}: {status} - {details_str}")
    
    # Delegate standard logging methods to the underlying logger
    def debug(self, message, *args, **kwargs):
        """Log a debug message."""
        return self.logger.debug(message, *args, **kwargs)
    
    def info(self, message, *args, **kwargs):
        """Log an info message."""
        return self.logger.info(message, *args, **kwargs)
    
    def warning(self, message, *args, **kwargs):
        """Log a warning message."""
        return self.logger.warning(message, *args, **kwargs)
    
    def error(self, message, *args, **kwargs):
        """Log an error message."""
        return self.logger.error(message, *args, **kwargs)
    
    def critical(self, message, *args, **kwargs):
        """Log a critical message."""
        return self.logger.critical(message, *args, **kwargs)


def get_etl_logger(name: str) -> ETLLogger:
    """Factory function to get a configured ETL logger."""
    return ETLLogger(name)


class ETLException(Exception):
    """Base exception class for ETL operations."""
    
    def __init__(self, message: str, operation: str, context: dict = None):
        self.operation = operation
        self.context = context or {}
        super().__init__(message)


class DataValidationError(ETLException):
    """Exception raised when data validation fails."""
    pass


class AWSServiceError(ETLException):
    """Exception raised when AWS service operations fail."""
    pass


class DataProcessingError(ETLException):
    """Exception raised during data processing operations."""
    pass
