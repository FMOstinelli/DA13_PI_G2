from setuptools import setup, find_packages

setup(
    name='heritage_etl_pipeline',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]',
        'pandas',  # Versión compatible con Beam
        'SQLAlchemy',
        'pymysql',
        'google-cloud-storage',
        'protobuf',  # Evita conflictos de versión
        'tenacity',   # Para reintentos
        'cryptography'  # Para conexiones seguras
    ],
    # Incluye archivos no-Python necesarios
    include_package_data=True,
    package_data={
        '': ['*.json', '*.sql']
    },
    python_requires='>=3.8,<3.13',  # Rango de versiones soportadas
    # Metadata adicional
    author="Tu Nombre",
    description="Pipeline ETL para Heritage Database",
    license="Apache License 2.0"
)