import logging
from neo4j import GraphDatabase, Driver
from .config import settings

logger = logging.getLogger(__name__)

class Neo4jConnection:
    def __init__(self):
        self._driver: Driver | None = None
        
    def connect(self):
        if not self._driver:
            try:
                self._driver = GraphDatabase.driver(
                    settings.NEO4J_URI,
                    auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
                )
                logger.info("Connected to Neo4j successfully.")
            except Exception as e:
                logger.error(f"Failed to connect to Neo4j: {e}")
                
    def close(self):
        if self._driver:
            self._driver.close()
            self._driver = None
            logger.info("Neo4j connection closed.")
            
    def get_driver(self) -> Driver:
        if not self._driver:
            self.connect()
        return self._driver

neo4j_conn = Neo4jConnection()

def get_neo4j_driver():
    return neo4j_conn.get_driver()
