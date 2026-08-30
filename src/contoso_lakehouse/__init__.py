"""Metadata-gedreven ETL framework voor het Contoso Lakehouse.

Submodules worden bewust niet eager geïmporteerd: ``context``, ``hashing`` en de
metadata-consistentietests draaien zo ook zonder PySpark (bijvoorbeeld in CI).
"""

__all__ = [
    "audit",
    "bronze",
    "context",
    "datavault",
    "gold",
    "hashing",
    "metadata",
    "orchestration",
    "quality",
    "seed",
    "validation",
]
__version__ = "0.1.0"
