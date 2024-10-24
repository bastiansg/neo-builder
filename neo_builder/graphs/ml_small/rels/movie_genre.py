from typing import Iterable

from neo_builder.db import Neo4jConnector
from neo_builder.build import BaseBuilder, BuilderRow

from ..nodes import MovieNodeBuilder, GenreNodeBuilder


class MovieGenreRelBuilder(BaseBuilder):
    def __init__(
        self,
        neo_connector: Neo4jConnector,
        data_items: list[dict],
        build_from_csv: bool = False,
        out_file_name: str = "movie-genre-rels.csv",
        src_node_type: str = "Movie",
        tgt_node_type: str = "Genre",
        rel_type: str = "HAS_GENRE",
        property_cast_map: dict = {
            "strength": "toFloat",
        },
    ):
        super().__init__(
            neo_connector=neo_connector,
            data_items=data_items,
            build_from_csv=build_from_csv,
            src_node_type=src_node_type,
            tgt_node_type=tgt_node_type,
            rel_type=rel_type,
            out_file_name=out_file_name,
            property_cast_map=property_cast_map,
        )

    def get_rows(self, data_item: dict) -> Iterable[BuilderRow]:
        title = data_item["title"]
        rel_rows = (
            BuilderRow(
                src_node_id=MovieNodeBuilder.get_node_id(title=title),
                tgt_node_id=GenreNodeBuilder.get_node_id(genre=genre),
                properties={"strength": 1.0},
            )
            for genre in data_item["genres"].split("|")
        )

        return rel_rows
