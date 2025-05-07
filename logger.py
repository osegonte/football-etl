"""
Logging module for the football data ETL pipeline.
"""

import logging
import os
from logging.handlers import RotatingFileHandler
import sys
from datetime import datetime

def setup_logger(name, log_file, level=logging.INFO):
    """Set up a logger with both file and console handlers.

    Args:
        name (str): Logger name
        log_file (str): Path to log file
        level (int): Logging level

    Returns:
        logging.Logger: Configured logger
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Ensure log directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Create file handler
    file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

class PipelineLogger:
    """Pipeline logger that provides standardized logging methods."""
    
    def __init__(self, name, log_file, level=logging.INFO):
        """Initialize the pipeline logger.
        
        Args:
            name (str): Logger name
            log_file (str): Path to log file
            level (int): Logging level
        """
        self.logger = setup_logger(name, log_file, level)
        
    def start_pipeline(self, pipeline_name):
        """Log pipeline start.
        
        Args:
            pipeline_name (str): Name of the pipeline
        """
        self.logger.info(f"{'='*20} STARTING {pipeline_name} PIPELINE {'='*20}")
        
    def end_pipeline(self, pipeline_name, stats=None):
        """Log pipeline end with optional statistics.
        
        Args:
            pipeline_name (str): Name of the pipeline
            stats (dict, optional): Pipeline statistics
        """
        if stats:
            self.logger.info(f"Pipeline statistics: {stats}")
        self.logger.info(f"{'='*20} COMPLETED {pipeline_name} PIPELINE {'='*20}")
        
    def start_job(self, job_name):
        """Log job start.
        
        Args:
            job_name (str): Name of the job
        """
        self.logger.info(f"{'-'*10} Starting job: {job_name} {'-'*10}")
        
    def end_job(self, job_name, stats=None):
        """Log job end with optional statistics.
        
        Args:
            job_name (str): Name of the job
            stats (dict, optional): Job statistics
        """
        if stats:
            self.logger.info(f"Job statistics: {stats}")
        self.logger.info(f"{'-'*10} Completed job: {job_name} {'-'*10}")
        
    def info(self, message):
        """Log info message.
        
        Args:
            message (str): Message to log
        """
        self.logger.info(message)
        
    def warning(self, message):
        """Log warning message.
        
        Args:
            message (str): Message to log
        """
        self.logger.warning(message)
        
    def error(self, message, exc_info=False):
        """Log error message.
        
        Args:
            message (str): Message to log
            exc_info (bool): Whether to include exception info
        """
        self.logger.error(message, exc_info=exc_info)
        
    def exception(self, message):
        """Log exception message.
        
        Args:
            message (str): Message to log
        """
        self.logger.exception(message)