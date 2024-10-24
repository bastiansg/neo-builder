import os
import asyncio

from rich.pretty import pprint
from neo4j import GraphDatabase

from common.logger import get_logger


logger = get_logger(__name__)


NEO4J_URI = os.getenv("NEO_URI", "bolt://localhost:7687")


# NOTE https://neo4j.com/docs/getting-started/languages-guides/neo4j-python/
class Neo4jConnector:
    def __init__(
        self,
        neo4j_uri: str = NEO4J_URI,
        connection_pool_size: int = 128,
    ):
        logger.debug(f"connecting to Neo4j at `{neo4j_uri}`")
        self.driver = GraphDatabase.driver(
            uri=neo4j_uri,
            auth=None,
            max_connection_pool_size=connection_pool_size,
        )

        self.driver.verify_connectivity()

    def __del__(self) -> None:
        self.driver.close()

    def run_query(
        self,
        query: str,
        **kwargs,
    ) -> dict:
        with self.driver.session() as session:
            print(f"executing query: \n{query}")
            result = session.run(query=query, **kwargs)
            result_data = result.data()

            counters = result.consume().counters
            pprint(counters)

            return {
                "data": result_data,
                "counters": counters,
            }

    async def async_run_query(
        self,
        query: str,
        **kwargs,
    ) -> dict:
        return await asyncio.to_thread(
            self.run_query,
            query=query,
            **kwargs,
        )
