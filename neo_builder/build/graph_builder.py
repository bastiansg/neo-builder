from typing import Literal
from pydantic import BaseModel, StrictStr, PositiveInt

from common.logger import get_logger
from common.utils.yaml_data import load_yaml

from neo_builder import conf
from neo_builder.db import Neo4jConnector

from .base_builder import BaseBuilder


logger = get_logger(__name__)


class GraphBuildMap(BaseModel, arbitrary_types_allowed=True):
    node_builders: list[BaseBuilder]
    rel_builders: list[BaseBuilder]


class Constraint(BaseModel):
    constraint_name: StrictStr
    node_type: StrictStr
    node_property: StrictStr


class Index(BaseModel):
    index_name: StrictStr
    node_type: StrictStr
    node_property: StrictStr


class VectorIndex(Index):
    vector_size: PositiveInt
    similarity_metric: Literal["cosine", "euclidean"] = "cosine"


class GraphBuilder:
    def __init__(
        self,
        neo_connector: Neo4jConnector,
        build_map: GraphBuildMap,
        constraints: list[Constraint],
        indexes: list[Index] = [],
        full_text_indexes: list[Index] = [],
        vector_indexes: list[VectorIndex] = [],
        build_query_path: str = f"{conf.__path__[0]}/graph-builder.yaml",
    ):
        self.neo_connector = neo_connector
        self.build_map = build_map

        self.create_constraints(
            constraints=constraints,
            base_query=load_yaml(build_query_path)["constraint"],
        )

        self.create_indexes(
            indexes=indexes,
            base_query=load_yaml(build_query_path)["index"],
        )

        self.create_indexes(
            indexes=full_text_indexes,
            base_query=load_yaml(build_query_path)["full-text-index"],
        )

        self.create_vector_indexes(
            vector_indexes=vector_indexes,
            base_query=load_yaml(build_query_path)["vector-index"],
        )

    def create_constraints(
        self,
        constraints: list[dict],
        base_query: str,
    ) -> None:
        for c in constraints:
            query = base_query.format(
                constraint_name=c["constraint_name"],
                node_type=c["node_type"],
                node_property=c["node_property"],
            )

            self.neo_connector.run_query(query=query)

    def create_indexes(self, indexes: list[dict], base_query: str) -> None:
        for i in indexes:
            query = base_query.format(
                index_name=i["index_name"],
                node_type=i["node_type"],
                node_property=i["node_property"],
            )

            self.neo_connector.run_query(query=query)

    def create_vector_indexes(
        self,
        vector_indexes: list[dict],
        base_query: str,
    ) -> None:
        for vi in vector_indexes:
            query = base_query.format(
                index_name=vi["index_name"],
                node_type=vi["node_type"],
                node_property=vi["node_property"],
                vector_size=vi["vector_size"],
                similarity_metric=vi["similarity_metric"],
            )

            self.neo_connector.run_query(query=query)

    def build(self) -> None:
        logger.info("building graph nodes")
        for node_builder in self.build_map.node_builders:
            _ = node_builder.create_nodes()

        logger.info("building graph relations")
        for rel_builder in self.build_map.rel_builders:
            _ = rel_builder.create_rels()
