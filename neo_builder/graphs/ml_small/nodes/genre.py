import numpy as np

from joblib import hash
from typing import Iterable

from neo_builder.db import Neo4jConnector
from neo_builder.build import BaseBuilder, BuilderRow


class GenreNodeBuilder(BaseBuilder):
    def __init__(
        self,
        neo_connector: Neo4jConnector,
        data_items: list[dict],
        build_from_csv: bool = False,
        node_type: str = "Genre",
        out_file_name: str = "genre-nodes.csv",
        property_cast_map: dict = {
            "genre": "toString",
            # NOTE: if build_from_csv is False, property_castmust be None
            # instead of apoc.convert.fromJsonList
            "fake_vector": None,
            # "fake_vector": "apoc.convert.fromJsonList",
        },
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
    def get_node_id(genre: str) -> str:
        return hash(f"movie-{genre}")

    def get_rows(self, data_item: dict) -> Iterable[BuilderRow]:
        return (
            BuilderRow(
                node_id=GenreNodeBuilder.get_node_id(genre=genre),
                node_type=self.node_type,
                properties={
                    "genre": genre,
                    "fake_vector": np.random.rand(16).tolist(),
                },
            )
            for genre in data_item["genres"].split("|")
        )
