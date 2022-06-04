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

    def load_csv(self, csv_path):
        with open(csv_path, 'rt', encoding='UTF-8') as f:
            reader = csv.reader(f)
            for idx, line in enumerate(reader):
                try:
                    query = line[0]
                    self.session.run(query)
                    print(f'----- DONE {idx + 1} query -----')
                except Exception as e:
                    print(e)
