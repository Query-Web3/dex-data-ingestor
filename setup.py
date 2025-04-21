from setuptools import setup, find_packages

setup(
    name="dex-data-ingestor",
    version="0.1.0",
    description="Ingest pool and transaction data from Bifrost, Hydration, etc. into a local database",
    author="QueryWeb3",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.0",
        "SQLAlchemy>=1.4.0",
        "PyMySQL>=1.0.2",
        "mysql-connector-python>=8.0.0",
        "APScheduler>=3.9.1",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "dex-ingest=dex_data_ingestor.src:main",  # 如果你有 CLI 脚本
        ],
    },
)
