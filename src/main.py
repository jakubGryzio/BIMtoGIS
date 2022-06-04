from src.Loader.Loader import Loader
from src.Storage.Neo4jHandler import Neo4jHandler

FILEPATH = '../model/AC20-Institute-Var-2.ifc'
CSV_PATH = "./ifc_objects.csv"

loader = Loader()
loader.file_loader(FILEPATH)
# loader.ifc_objects_provider(CSV_PATH)

neo = Neo4jHandler()
neo.load_csv(CSV_PATH)
