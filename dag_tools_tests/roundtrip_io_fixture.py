"""A tiny IO manager the round-trip generator tests import by FQN."""
from dagster import IOManager


class StubFrame:
    """Looks enough like a table for the probe to read columns and length."""

    def __init__(self, columns=("a", "b"), rows=3):
        self.columns = list(columns)
        self._rows = rows

    def __len__(self):
        return self._rows


class FixtureIOManager(IOManager):
    """Loads a StubFrame; records the partition key each load was asked for."""

    LOADED_PARTITIONS: list = []

    def __init__(self, columns=("a", "b"), return_none=False):
        self._columns = columns
        self._return_none = return_none

    def handle_output(self, context, obj):
        raise AssertionError("the round-trip probe must never write")

    def load_input(self, context):
        pk = context.partition_key if context.has_partition_key else None
        FixtureIOManager.LOADED_PARTITIONS.append(pk)
        return None if self._return_none else StubFrame(self._columns)
