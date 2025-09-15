MATCH (n)-[r]->(m)
RETURN n, r, m
LIMIT 1000;

MATCH (n:Entity)
RETURN n;


MATCH (n:Agent)
RETURN n;