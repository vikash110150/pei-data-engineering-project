"""
Configuration management utility for the PEI Data Engineering Project.
Loads and provides access to configuration parameters.
"""

import yaml
import os
from typing import Dict, Any


class Config:
    """Configuration manager for the project."""

    def __init__(self, config_path: str = None):
        """
        Initialize configuration.

        Args:
            config_path: Path to config.yaml file
        """
        if config_path is None:
            # Default path when running in Databricks
            config_path = "/Workspace/Users/vikash110150@gmail.com/pei-data-engineering-project/config/config.yaml"

        self.config_path = config_path
        self.config_data = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Return default configuration if file not found
            return self._default_config()

    def _default_config(self) -> Dict[str, Any]:
        """Return default configuration."""
        return {
            'data_paths': {
                'raw_base_path': '/Workspace/Users/vikash110150@gmail.com/raw_data/raw',
                'bronze_base_path': 'workspace.default.bronze_',
                'silver_base_path': 'workspace.default.silver_',
                'gold_base_path': 'workspace.default.gold_'
            },

            'tables': {
                'bronze': {
                    'orders': "workspace.default.bronze_orders",
                    'products': "workspace.default.bronze_products",
                    'customers': "workspace.default.bronze_customers"
                },
                'silver': {
                    'orders': "workspace.default.silver_orders",
                    'products': "workspace.default.silver_products",
                    'customers': "workspace.default.silver_customers"
                },
                'gold': {
                    'orders': "workspace.default.gold_orders",
                    'profit': "workspace.default.gold_profit"
                }
            },

            'source_files': {
                'orders': "Orders.json",
                'products': "Products.csv",
                'customers': "Customer.xlsx"
            }
        }


    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.

        Args:
            key_path: Dot-separated path (e.g., 'data_paths.raw_base_path')
            default: Default value if key not found

        Returns:
            Configuration value
        """
        keys = key_path.split('.')
        value = self.config_data

        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default

        return value

    def get_raw_path(self, filename: str = "") -> str:
        """Get full path for raw data file."""
        base = self.get('data_paths.raw_base_path')
        return f"{base}/{filename}" if filename else base

    def get_bronze_path(self, table_name: str = "") -> str:
        """Get full path for bronze layer table."""
        base = self.get('data_paths.bronze_base_path')
        return f"{base}/{table_name}" if table_name else base

    def get_silver_path(self, table_name: str = "") -> str:
        """Get full path for silver layer table."""
        base = self.get('data_paths.silver_base_path')
        return f"{base}/{table_name}" if table_name else base

    def get_gold_path(self, table_name: str = "") -> str:
        """Get full path for gold layer table."""
        base = self.get('data_paths.gold_base_path')
        return f"{base}/{table_name}" if table_name else base


# Global configuration instance
_config_instance = None


def get_config(config_path: str = None) -> Config:
    """
    Get or create global configuration instance.

    Args:
        config_path: Path to config file (optional)

    Returns:
        Config instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = Config(config_path)
    return _config_instance
