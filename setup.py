# File: setup.py
# Optional: install src/ as a Python package on the cluster.
# Usage: %pip install /Workspace/Repos/ddda-clinical-platform-koios/dt-drugdevelopment-dna/dist/ddda-1.0.0-py3-none-any.whl

from setuptools import setup, find_packages

setup(
    name="ddda-clinical-platform",
    version="1.0.0",
    description="DDDA Clinical Data Platform — Databricks ETL Package",
    author="Data Engineering",
    author_email="",   # set via package metadata at release time, not in source
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=[
        "pyyaml>=6.0",
        "boto3>=1.26.0",
        "delta-spark==3.1.0",
        "databricks-labs-dqx>=0.2.0",
        "openpyxl>=3.1.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
