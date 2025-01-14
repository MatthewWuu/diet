from py2neo import Graph
import json,os
from py2neo import Graph,Node,Relationship
from py2neo.bulk import create_nodes

class dbneo:
# using BOLT
    def __init__(self) -> None:
        self.graph = Graph("neo4j://localhost:7687", auth=("neo4j", "123456789"))


    def runCQL(self,cql):
        self.graph.run(cql)

    #dict={"Key":value}
    def createNode(self,label,labelvalue,dict):
        node=Node(label,name=labelvalue)
        for k,v in dict.items():
            node[k]=v
        self.graph.merge(node,str(label),"name")
        return node

    #dict={start:{label:xxxx value:xxxxx},end:{label:xxxx,value:xxxxxxx}}
    def createRelation(self,label,dict):
        start_label=dict["start"]["label"]
        end_label=dict["end"]["label"]

        start_node=Node(start_label,name=dict["start"]["value"])
        end_node=Node(end_label,name=dict["end"]["value"])

        relation = Relationship(start_node, label, end_node)
        self.graph.merge(relation)

    def createRelationFromStartToEnd(self,label,start,end,dict):
        # start_label=dict["start"]["label"]
        # end_label=dict["end"]["label"]

        # start_node=Node(start_label,name=dict["start"]["value"])
        # end_node=Node(end_label,name=dict["end"]["value"])

        relation = Relationship(start, label, end)
        if dict:
            for k,v in dict.items():
                relation[k]=v
        self.graph.merge(relation)

    def QueryNodes(self):
        cql= " match (m) return m "
        datas=self.graph.run(cql).data()
        return datas

    def QueryRelation(self):
        cql="match (m)-[r]->(n) return type(r) as relation_name,m.name as startname,n.name as endname"
        datas=self.graph.run(cql).data()
        return datas

