from neo4j import GraphDatabase
import csv


class Neo4jHandler:
    HOST = "bolt://localhost:7687"
    AUTH = ('neo4j', 'test')

    def __init__(self):
        self.session = self.connect()

    def connect(self):
        driver = GraphDatabase.driver(self.HOST, auth=self.AUTH)
        return driver.session()

    def load_csv_queries(self, csv_path):
        with open(csv_path, 'rt', encoding='UTF-8') as f:
            reader = csv.reader(f)
            for idx, line in enumerate(reader):
                try:
                    query = line[0]
                    self.session.run(query)
                    print(f'----- DONE {idx + 1} query -----')
                except Exception as e:
                    print(e)

    def select_node_by_label(self, label):
        query = "MATCH (node {{label: '{}'}}) RETURN node.id, node.attributes".format(label)
        return self.session.run(query)

    def select_node_by_id(self, id):
        query = "MATCH (node {{id: '{}'}}) RETURN node".format(id)
        return self.session.run(query)

    def select_node_by_id_in(self, ids):
        query = "MATCH (node) WHERE node.id IN [{}] RETURN node".format(ids)
        return self.session.run(query)

    def create_relationships(self):
        FIRST_SEPARATOR = '('
        SECOND_SEPARATOR = ','
        STRIP_ELEMENT = ')'
        LABEL = 'IFCRELAGGREGATES'
        result = self.select_node_by_label(LABEL)
        for node in result:
            attributes = node[1].rsplit(FIRST_SEPARATOR)
            relatingId = attributes[0].split(SECOND_SEPARATOR)[-2]
            split_relatingId_elements = attributes[-1].rstrip(STRIP_ELEMENT).split(SECOND_SEPARATOR)
            relatedId = SECOND_SEPARATOR.join("'" + elem + "'" for elem in split_relatingId_elements)
            relating_query = "MATCH (r:ifcID {{id: '{}'}}), (n:ifcID {{id: '{}'}}) CREATE (r)-[:RELATING]->(n);".format(node[0], relatingId)
            related_query = "MATCH (r:ifcID {{id: '{}'}}), (n:ifcID) WHERE n.id IN [{}] CREATE (r)-[:RELATED]->(n);".format(node[0], relatedId)
            self.session.run(relating_query)
            self.session.run(related_query)
            print(node)

