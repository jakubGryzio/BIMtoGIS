from Storage.Neo4jHandler import Neo4jHandler


class SpatialRelationship(Neo4jHandler):
    def __init__(self):
        super().__init__()

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
            self.create_axisPlacement_cartesianPoint_relationship(node)
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
