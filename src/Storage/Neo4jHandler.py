from neo4j import GraphDatabase
import csv


class Neo4jHandler:
    HOST = "bolt://localhost:7687"
    AUTH = ('neo4j', 'test')

    def __init__(self):
        self.session = self.connect()

    @staticmethod
    def extract_attribute(node, separator):
        ATTRIBUTE_INDX = 2
        return node[ATTRIBUTE_INDX].rsplit(separator)

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
        query = "MATCH (node {{label: '{}'}}) RETURN node.id, node.label, node.attributes".format(label)
        return self.session.run(query)

    def select_node_by_label_with_limit(self, label, limit):
        query = "MATCH (node {{label: '{}'}}) RETURN node.id, node.label, node.attributes LIMIT {}".format(label, limit)
        return self.session.run(query)

