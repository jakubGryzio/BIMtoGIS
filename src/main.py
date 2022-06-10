from Loader.Loader import Loader
from Storage.Neo4jHandler import Neo4jHandler

FILEPATH = '../model/AC20-Institute-Var-2.ifc'
CSV_PATH = "./ifc_objects.csv"

def load_ifc_file():
    loader = Loader()
    loader.file_loader(FILEPATH)
    loader.ifc_objects_provider(CSV_PATH)

def import_ifc_object():
    neo = Neo4jHandler()
    neo.load_csv_queries(CSV_PATH)

def neo4j_handler():
    neo = Neo4jHandler()

if __name__ == '__main__':
    neo4j_handler()
