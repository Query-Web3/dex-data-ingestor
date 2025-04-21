from setuptools import setup, find_packages

setup(
    name="dex-data-ingestor",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests",
        "sqlalchemy",
        "pydantic",
        "python-dotenv",
        "psycopg2-binary"
    ],
    entry_points={
        "console_scripts": [
            "dex-ingest=main:main"
        ],
    },
)
