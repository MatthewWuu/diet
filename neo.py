from py2neo import Graph, Node, Relationship
from py2neo.bulk import merge_nodes, merge_relationships

class dbneo:
    def __init__(self):
        self.graph = Graph("neo4j://localhost:7687", auth=("neo4j", "123456789"))

    def runCQL(self, cql):
        self.graph.run(cql)

    # 数据类型转换函数
    def convert_to_python_types(self, data):
        return {
            k: (float(v) if isinstance(v, (int, float)) else str(v) if v is not None else None)
            for k, v in data.items()
        }

    # 创建节点
    def createNode(self, label, labelvalue, attributes):
        attributes = self.convert_to_python_types(attributes)
        node = Node(label, name=labelvalue, **attributes)
        self.graph.merge(node, label, "name")
        return node

    # 创建关系
    def createRelationFromStartToEnd(self, label, start, end, attributes):
        attributes = self.convert_to_python_types(attributes)
        relation = Relationship(start, label, end, **attributes)
        self.graph.merge(relation)
