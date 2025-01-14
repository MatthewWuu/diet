from py2neo import Graph

try:
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "your_password"))
    result = graph.run("RETURN 'py2neo connection successful' AS message")
    for record in result:
        print(record["message"])
except Exception as e:
    print(f"Failed to connect: {e}")
