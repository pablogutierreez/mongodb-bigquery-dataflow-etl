"""Packaging metadata so Dataflow workers receive the local helper modules.

When the pipeline runs on Dataflow, each worker needs `transformations.py`
and `metrics.py` available for import. Referencing this file via
`--setup_file ./setup.py` (set in main.py) makes Beam stage them.
"""
from setuptools import setup

setup(
    name='mongodb-bigquery-dataflow-etl',
    version='1.0.0',
    py_modules=['transformations', 'metrics'],
)
