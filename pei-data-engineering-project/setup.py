"""Setup script for PEI Data Engineering Project."""

from setuptools import setup, find_packages

setup(
    name="pei-data-engineering-project",
    version="1.0.0",
    description="E-commerce Sales Data Processing with Databricks",
    author="PEI Data Engineering Team",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.4.0",
        "pandas>=2.0.0",
        "openpyxl>=3.1.0",
        "pytest>=7.4.0",
        "pyyaml>=6.0",
    ],
    python_requires=">=3.9",
)
