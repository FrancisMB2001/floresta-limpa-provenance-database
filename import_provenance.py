from neo4j import GraphDatabase
import json

# Connect to Neo4j
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "floresta_limpa_2025"))

# Load JSONL data
with open("provenance_nodes.jsonl") as f:
    nodes = [json.loads(line) for line in f]

with open("provenance_relations.jsonl") as f:
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
