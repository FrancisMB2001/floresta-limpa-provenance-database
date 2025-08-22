import os
from neo4j import GraphDatabase
import json
from dotenv import load_dotenv

load_dotenv()


NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "floresta_limpa_2025")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_CONNECTION = os.getenv("NEO4J_CONNECTION", "bolt://localhost:7687")

NODES_LOCATION = os.getenv("NODES_LOCATION", "provenance_nodes.jsonl")
RELATIONS_LOCATION = os.getenv("RELATIONS_LOCATION", "provenance_relations.jsonl")

# Connect to Neo4j
driver = GraphDatabase.driver(NEO4J_CONNECTION, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Load JSONL data
with open(NODES_LOCATION) as f:
    nodes = [json.loads(line) for line in f]

with open(RELATIONS_LOCATION) as f:
    relations = [json.loads(line) for line in f]

def insert_nodes(tx, node):
    # Take all properties except id and type
    props = {k: v for k, v in node.items() if k not in ["id", "type"]}

    # Ensure a displayName for visualization
    props["displayName"] = node["id"]

    tx.run(
        f"MERGE (n:{node['type']} {{id:$id}}) "
        f"SET n += $props",
        id=node["id"],
        props=props
    )

def insert_relations(tx, rel):
    # Use all fields except id, relationType, source, target as attributes
    attrs = {k: v for k, v in rel.items() if k not in ["id", "relationType", "source", "target"]}
    tx.run(
        f"MATCH (a {{id: $source}}), (b {{id: $target}}) "
        f"MERGE (a)-[r:{rel['relationType']}]->(b) "
        f"SET r += $attrs",
        source=rel["source"],
        target=rel["target"],
        attrs=attrs
    )

with driver.session() as session:
    for node in nodes:
        session.execute_write(insert_nodes, node)
    for rel in relations:
        session.execute_write(insert_relations, rel)

driver.close()
