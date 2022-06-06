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

    def create_project_relationship(self):
        FIRST_SEPARATOR = '('
        SECOND_SEPARATOR = ','
        STRIP_ELEMENT = ')'
        LABEL = 'IFCRELAGGREGATES'
        result = self.select_node_by_label(LABEL)
        for node in result:
            attributes = self.extract_attribute(node, FIRST_SEPARATOR)
            relatingId = attributes[0].split(SECOND_SEPARATOR)[-2]
            split_relatedId_elements = attributes[-1].rstrip(STRIP_ELEMENT).split(SECOND_SEPARATOR)
            relatedId = SECOND_SEPARATOR.join("'" + elem + "'" for elem in split_relatedId_elements)
            relating_query = "MATCH (r:ifcID {{id: '{}'}}), (n:ifcID {{id: '{}'}}) MERGE (r)-[:RELATING]->(n);".format(
                node[0], relatingId)
            related_query = "MATCH (r:ifcID {{id: '{}'}}), (n:ifcID) WHERE n.id IN [{}] MERGE (r)-[:RELATED]->(n);".format(
                node[0], relatedId)
            self.session.run(relating_query)
            self.session.run(related_query)
            print(relating_query)
            print(related_query)

    def create_spatial_relationship(self):
        self.create_localPlacement_ifc_relationship()
        self.create_between_localPlacement_and_axisPlacement_relationship()
        self.create_axisPlacement_relationship()

    def create_localPlacement_ifc_relationship(self):
        NODE_LABELS = ['IFCSITE', 'IFCBUILDING', 'IFCBUILDINGSTOREY', 'IFCSPACE']
        SEPARATOR = ','
        LOCALPLACEMENT_INDX = 5
        for label in NODE_LABELS:
            result = self.select_node_by_label(label)
            for node in result:
                attributes = self.extract_attribute(node, SEPARATOR)
                localPlacementId = attributes[LOCALPLACEMENT_INDX]
                localPlacementRelationQuery = "MATCH (lp:ifcID {{id: '{}'}}), (n:ifcID {{id: '{}'}}) MERGE (n)-[:PLACED_BY]->(lp);".format(
                    localPlacementId, node[0])
                self.session.run(localPlacementRelationQuery)
                print(localPlacementRelationQuery)

    def create_between_localPlacement_and_axisPlacement_relationship(self):
        LABEL = 'IFCLOCALPLACEMENT'
        result = self.select_node_by_label(LABEL)
        for node in result:
            self.create_localPlacement_localPlacement_relationship(node)
            self.create_localPlacement_axisPlacement_relationship(node)

    def create_localPlacement_axisPlacement_relationship(self, node):
        SEPARATOR = ','
        attributes = self.extract_attribute(node, SEPARATOR)
        related_axisPlacement = attributes[1]
        axisPlacementRelationQuery = "MATCH (l:ifcID {{id: '{}'}}), (a:ifcID {{id: '{}'}}) MERGE (l)-[:RELATIVE_PLACEMENT]->(a);".format(
            node[0], related_axisPlacement)
        self.session.run(axisPlacementRelationQuery)
        print(axisPlacementRelationQuery)

    def create_localPlacement_localPlacement_relationship(self, node):
        SEPARATOR = ','
        NULL_VALUE = '$'
        attributes = self.extract_attribute(node, SEPARATOR)
        related_localPlacementId = attributes[0]
        if related_localPlacementId == NULL_VALUE:
            return
        localPlacementRelationQuery = "MATCH (ch:ifcID {{id: '{}'}}), (p:ifcID {{id: '{}'}}) MERGE (ch)-[:RELATED_TO]->(p);".format(
            node[0], related_localPlacementId)
        self.session.run(localPlacementRelationQuery)
        print(localPlacementRelationQuery)

    def create_axisPlacement_relationship(self):
        LABEL = 'IFCAXIS2PLACEMENT3D'
        result = self.select_node_by_label(LABEL)
        for node in result:
            # self.create_axisPlacement_cartesianPoint_relationship(node)
            self.create_axisPlacement_direction_relationship(node)

    def create_axisPlacement_cartesianPoint_relationship(self, node):
        SEPARATOR = ','
        attributes = self.extract_attribute(node, SEPARATOR)
        cartesianPointId = attributes[0]
        axisPlacementRelationQuery = "MATCH (a:ifcID {{id: '{}'}}), (c:ifcID {{id: '{}'}}) MERGE (a)-[:LOCATED_BY]->(c);".format(
            node[0], cartesianPointId)
        self.session.run(axisPlacementRelationQuery)
        print(axisPlacementRelationQuery)

    def create_axisPlacement_direction_relationship(self, node):
        SEPARATOR = ','
        attributes = self.extract_attribute(node, SEPARATOR)
        axisDirectionId = attributes[1]
        refDirectionId = attributes[2]
        axisDirectionRelationQuery = "MATCH (a:ifcID {{id: '{}'}}), (da:ifcID {{id: '{}'}}) MERGE (a)-[:AXIS]->(da);".format(
            node[0], axisDirectionId)
        refDirectionRelationQuery = "MATCH (a:ifcID {{id: '{}'}}), (dr:ifcID {{id: '{}'}}) MERGE (a)-[:REF_DIRECTION]->(dr);".format(
            node[0], refDirectionId)
        self.session.run(axisDirectionRelationQuery)
        self.session.run(refDirectionRelationQuery)
        print(axisDirectionRelationQuery)
        print(refDirectionRelationQuery)

    def create_geometry_relationship(self):
        pass

