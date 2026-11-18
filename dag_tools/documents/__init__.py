"""Documents to tables: a config-driven engine that turns bundles of
structured documents into one table per mapped entity.

Everything format-specific lives in a mapping YAML the deployment supplies;
nothing in this package knows any document format. The engine, the sources
and the Lance writer import no Dagster -- ``dag_tools.components.
document_tables`` is the Dagster surface over them.

Optional dependencies ride the ``documents`` extra (lxml, pylance, Pillow,
boto3, pyarrow); each module imports its dependency lazily and names the
extra when it is missing.
"""
