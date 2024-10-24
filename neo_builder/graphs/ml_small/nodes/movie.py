from joblib import hash
from typing import Iterable

from neo_builder.db import Neo4jConnector
from neo_builder.build import BaseBuilder, BuilderRow


class MovieNodeBuilder(BaseBuilder):
    def __init__(
        self,
        neo_connector: Neo4jConnector,
        data_items: list[dict],
        build_from_csv: bool = False,
        node_type: str = "Movie",
        out_file_name: str = "movie-nodes.csv",
        property_cast_map: dict = {"title": "toString"},
    ):
        super().__init__(
            neo_connector=neo_connector,
            data_items=data_items,
            build_from_csv=build_from_csv,
            node_type=node_type,
            out_file_name=out_file_name,
            property_cast_map=property_cast_map,
        )

    @staticmethod
    def get_node_id(title: str) -> str:
        return hash(f"movie-{title}")

    def get_rows(self, data_item: dict) -> Iterable[BuilderRow]:
        title = data_item["title"]
        node_row = BuilderRow(
            node_id=MovieNodeBuilder.get_node_id(title=title),
            node_type=self.node_type,
            properties={"title": title},
        )

        return [node_row]
