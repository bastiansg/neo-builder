import pandas as pd

from tqdm import tqdm
from itertools import chain
from neo4j import SummaryCounters
from more_itertools import flatten, unique_everseen

from typing import Any, Iterable
from abc import ABC, abstractmethod
from pydantic import BaseModel, StrictStr

from common.logger import get_logger
from common.utils.yaml_data import load_yaml

from neo_builder import conf
from neo_builder.db import Neo4jConnector


logger = get_logger(__name__)


class BuilderRow(BaseModel):
    node_id: StrictStr | None = None
    src_node_id: StrictStr | None = None
    tgt_node_id: StrictStr | None = None
    properties: dict


class BaseBuilder(ABC):
    def __init__(
        self,
        neo_connector: Neo4jConnector,
        out_file_name: str,
        data_items: Iterable[dict],
        property_cast_map: dict,
        node_type: str | None = None,
        rel_type: str | None = None,
        build_from_csv: bool = False,
        src_node_type: str | None = None,
        tgt_node_type: str | None = None,
        out_base_path: str = "/resources/db/neo4j/import",
        id_property: str = "node_id",
        query_row_name: str = "row",
        query_node_name: str = "node",
        query_rel_name: str = "rel",
        indent_spaces: int = 8,
        cypher_path: str = f"{conf.__path__[0]}/graph-builder.yaml",
        transaction_size: int = 10_000,
    ):
        self.neo_connector = neo_connector
        self.src_node_type = src_node_type
        self.tgt_node_type = tgt_node_type
        self.node_type = node_type
        self.rel_type = rel_type
        self.out_file_name = out_file_name
        self.out_path = f"{out_base_path}/{out_file_name}"
        self.data_items = data_items
        self.property_cast_map = property_cast_map | {"node_id": "toString"}
        self.build_from_csv = build_from_csv

        self.id_property = id_property
        self.query_row_name = query_row_name
        self.query_node_name = query_node_name
        self.query_rel_name = query_rel_name
        self.indent_spaces = indent_spaces
        self.transaction_size = transaction_size
        self.base_queries = load_yaml(cypher_path)

    @abstractmethod
    def get_rows(self, data_item: Any) -> Iterable[BuilderRow]:
        pass

    def _get_property_fragment(
        self,
        property_cast_map: dict,
        row_name: str,
        indent_spaces: int,
        separator: str,
        name: str,
    ) -> str:
        property_lines = (
            (
                f"{name}{prop_name}{separator}"
                f"{property_cast}({row_name}.{prop_name}),"
            )
            if property_cast is not None
            else f"{name}{prop_name}{separator}{row_name}.{prop_name},"
            for prop_name, property_cast in property_cast_map.items()
        )

        property_fragment = f"\n{' ' * indent_spaces}".join(property_lines)[:-1]
        return property_fragment

    def _get_rows(self) -> list[BuilderRow]:
        build_type = self.node_type if self.rel_type is None else self.rel_type
        logger.info(f"generating {build_type} rows")
        rows = flatten(
            map(
                self.get_rows,
                tqdm(
                    self.data_items,
                    ascii=" ##",
                    colour="#808080",
                ),
            )
        )

        first_row = next(rows, None)
        if first_row is None:
            return []

        property_keys = set(k for k in first_row.properties.keys())
        property_cast_map_keys = set(self.property_cast_map.keys()) - {
            "node_id"
        }

        assert property_keys == property_cast_map_keys, (
            "property keys and property_cast_map keys doesn't match:"
            f" {property_keys} != {property_cast_map_keys}"
        )

        rows = chain([first_row], rows)
        rows = (row.model_dump() for row in rows)
        rows = [row | row.pop("properties") for row in rows]

        return rows

    def _get_head_creation_query(self, rows: list[dict]) -> str:
        if self.build_from_csv:
            df_nodes = pd.DataFrame(rows)
            logger.info(f"writing csv => {self.out_path}")
            df_nodes.to_csv(
                self.out_path,
                index=False,
                # chunksize=10_000,
                doublequote=False,
                escapechar="\\",
            )

            head_creation_query = self.base_queries[
                "csv-head-creation-query"
            ].format(
                file_input=self.out_file_name,
                row_name=self.query_row_name,
            )

            return head_creation_query

        head_creation_query = self.base_queries["head-creation-query"].format(
            row_name=self.query_row_name,
        )

        return head_creation_query

    def create_nodes(self) -> SummaryCounters:
        rows = self._get_rows()
        if not rows:
            return SummaryCounters(statistics={})

        rows = list(unique_everseen(rows, key=lambda row: row["node_id"]))
        head_creation_query = self._get_head_creation_query(rows=rows)

        # NOTE: MERGE opration can not be performed when building from csv
        creation_query = (
            self.base_queries["node-create-query"].format(
                row_name=self.query_row_name,
                node_type=self.node_type,
                property_fragment=self._get_property_fragment(
                    property_cast_map=self.property_cast_map,
                    row_name=self.query_row_name,
                    indent_spaces=self.indent_spaces,
                    separator=": ",
                    name="",
                ),
            )
            if self.build_from_csv
            else self.base_queries["node-merge-query"].format(
                row_name=self.query_row_name,
                node_name=self.query_node_name,
                node_type=self.node_type,
                property_fragment=self._get_property_fragment(
                    property_cast_map=self.property_cast_map,
                    row_name=self.query_row_name,
                    indent_spaces=self.indent_spaces,
                    separator=" = ",
                    name=f"{self.query_node_name}.",
                ),
            )
        )

        creation_query = head_creation_query + creation_query
        result = self.neo_connector.run_query(
            query=creation_query,
            rows=None if self.build_from_csv else rows,
            transaction_size=self.transaction_size,
        )

        return result["counters"]

    def create_rels(self) -> SummaryCounters:
        rows = self._get_rows()
        if not rows:
            return SummaryCounters(statistics={})

        rows = list(
            unique_everseen(
                rows,
                key=lambda row: (
                    row["src_node_id"],
                    row["tgt_node_id"],
                ),
            )
        )

        head_creation_query = self._get_head_creation_query(rows=rows)

        # NOTE: MERGE opration can not be performed when building from csv
        creation_query = (
            self.base_queries["rel-create-query"].format(
                row_name=self.query_row_name,
                id_property=self.id_property,
                src_node_type=self.src_node_type,
                tgt_node_type=self.tgt_node_type,
                rel_type=self.rel_type,
                property_fragment=self._get_property_fragment(
                    property_cast_map=self.property_cast_map,
                    row_name=self.query_row_name,
                    indent_spaces=self.indent_spaces,
                    separator=": ",
                    name="",
                ),
            )
            if self.build_from_csv
            else self.base_queries["rel-merge-query"].format(
                row_name=self.query_row_name,
                id_property=self.id_property,
                rel_name=self.query_rel_name,
                src_node_type=self.src_node_type,
                tgt_node_type=self.tgt_node_type,
                rel_type=self.rel_type,
                property_fragment=self._get_property_fragment(
                    property_cast_map=self.property_cast_map,
                    row_name=self.query_row_name,
                    indent_spaces=self.indent_spaces,
                    separator=" = ",
                    name=f"{self.query_rel_name}.",
                ),
            )
        )

        creation_query = head_creation_query + creation_query
        result = self.neo_connector.run_query(
            query=creation_query,
            rows=None if self.build_from_csv else rows,
            transaction_size=self.transaction_size,
        )

        return result["counters"]
