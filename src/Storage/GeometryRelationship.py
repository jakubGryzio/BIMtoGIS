from Storage.Neo4jHandler import Neo4jHandler


class GeometryRelationship(Neo4jHandler):
    def __init__(self):
        super().__init__()

    def create_geometry_relationship(self):
        # self.create_productDefinition_relationship()
        # self.create_geometricCurve_relationship()
        # self.create_boundingBox_relationship()
        self.create_boundingBox_cartesian_relationship()
        # self.create_polyline_relationship()
        # self.create_polyline_cartesianPoint_relationship()

    def create_productDefinition_relationship(self):
        NODE_LABELS = ['IFCSITE', 'IFCBUILDING', 'IFCBUILDINGSTOREY', 'IFCSPACE']
        SEPARATOR = ','
        PRODUCT_DEFINITION_IDX = 6
        for label in NODE_LABELS:
            result = self.select_node_by_label(label)
            for node in result:
                attributes = self.extract_attribute(node, SEPARATOR)
                productDefinitionId = attributes[PRODUCT_DEFINITION_IDX]
                if productDefinitionId != '$':
                    productDefinitionRelationQuery = "MATCH (pd:ifcID {{id: '{}'}}), (n:ifcID {{id: '{}'}}) MERGE (n)-[:REPRESENTED_BY]->(pd);".format(
                        productDefinitionId, node[0])
                    self.session.run(productDefinitionRelationQuery)
                    print(productDefinitionRelationQuery)
                    self.create_shapeRepresentation_relationship(productDefinitionId)

    def create_shapeRepresentation_relationship(self, id):
        FIRST_SEPARATOR = '('
        SECOND_SEPARATOR = ','
        result = self.select_node_by_id(id)
        for node in result:
            attributes = self.extract_attribute(node, FIRST_SEPARATOR)
            split_shapeRepresentationIds = attributes[-1].rstrip(')').split(SECOND_SEPARATOR)
            shapeRepresentationIds = SECOND_SEPARATOR.join("'" + elem + "'" for elem in split_shapeRepresentationIds)
            shapeRepresentationQuery = "MATCH (sr:ifcID) WHERE sr.id IN [{}] RETURN sr.id, sr.attributes;".format(
                shapeRepresentationIds)
            self.select_only_geometric_shapeRepresentation(shapeRepresentationQuery)

    def select_only_geometric_shapeRepresentation(self, query):
        BOUNDING_BOX = 'BoundingBox'
        CURVE_SET = 'GeometricCurveSet'
        result = self.session.run(query)
        for shapeRepresentationNode in result:
            shapeRepresentationId = shapeRepresentationNode[0]
            attributes = shapeRepresentationNode[1]
            if BOUNDING_BOX in attributes or CURVE_SET in attributes:
                shapeRepresentationRelationQuery = "MATCH (pd:ifcID {{id: '{}'}}), (sr:ifcID {{id: '{}'}}) MERGE (pd)-[:REPRESENTED_BY]->(sr);".format(
                    id, shapeRepresentationId)
                self.session.run(shapeRepresentationRelationQuery)
                print(shapeRepresentationRelationQuery)

    def create_geometricCurve_relationship(self):
        CURVE_SET = 'GeometricCurveSet'
        SEPARATOR = ','
        shapeRepresentationQuery = 'MATCH (pd:ifcID {label:"IFCPRODUCTDEFINITIONSHAPE"})-[r:REPRESENTED_BY]->(sr:ifcID {label: "IFCSHAPEREPRESENTATION"}) RETURN sr.id, sr.attributes;'
        result = self.session.run(shapeRepresentationQuery)
        for node in result:
            id = node[0]
            attributes = node[1]
            if CURVE_SET in attributes:
                geometricCurveSetId = attributes.split(SEPARATOR)[-1].lstrip('(').rstrip(')')
                geometricCurveSetQuery = "MATCH (sr:ifcID {{id: '{}'}}), (gc:ifcID {{id: '{}'}}) MERGE (sr)-[:SWEPT_BY]->(gc);".format(
                    id, geometricCurveSetId)
                self.session.run(geometricCurveSetQuery)
                print(geometricCurveSetQuery)

    def create_boundingBox_relationship(self):
        BOUNDING_BOX = 'BoundingBox'
        SEPARATOR = ','
        shapeRepresentationQuery = 'MATCH (pd:ifcID {label:"IFCPRODUCTDEFINITIONSHAPE"})-[r:REPRESENTED_BY]->(sr:ifcID {label: "IFCSHAPEREPRESENTATION"}) RETURN sr.id, sr.attributes;'
        result = self.session.run(shapeRepresentationQuery)
        for node in result:
            id = node[0]
            attributes = node[1]
            if BOUNDING_BOX in attributes:
                boundingBoxId = attributes.split(SEPARATOR)[-1].lstrip('(').rstrip(')')
                boundingBoxQuery = "MATCH (sr:ifcID {{id: '{}'}}), (bb:ifcID {{id: '{}'}}) MERGE (sr)-[:DEFINED_BY]->(bb);".format(
                    id, boundingBoxId)
                self.session.run(boundingBoxQuery)
                print(boundingBoxQuery)

    def create_boundingBox_cartesian_relationship(self):
        SEPARATOR = ','
        boundingBoxQuery = 'MATCH (sr:ifcID {label: "IFCSHAPEREPRESENTATION"})-[:DEFINED_BY]->(bb:ifcID {label: "IFCBOUNDINGBOX"}) RETURN bb.id, bb.attributes;'
        result = self.session.run(boundingBoxQuery)
        for node in result:
            id = node[0]
            attributes = node[1]
            cartesianPointId = attributes.split(SEPARATOR)[0]
            cartesianPointQuery = "MATCH (bb:ifcID {{id: '{}'}}), (c:ifcID {{id: '{}'}}) MERGE (bb)-[:CONTAINS]->(c);".format(id, cartesianPointId)
            self.session.run(cartesianPointQuery)
            print(cartesianPointQuery)

    def create_polyline_relationship(self):
        geometricCurveSetQuery = 'MATCH (sr:ifcID)-[:SWEPT_BY]->(gc:ifcID) RETURN gc.id, gc.attributes;'
        result = self.session.run(geometricCurveSetQuery)
        for node in result:
            geometricCurveSetId = node[0]
            attribute = node[1]
            polylineId = attribute.lstrip('(').rstrip(')')
            polylineQuery = "MATCH (gc:ifcID {{id: '{}'}}), (p:ifcID {{id: '{}'}}) MERGE (gc)-[:DEFINED_BY]->(p);".format(
                geometricCurveSetId, polylineId)
            self.session.run(polylineQuery)
            print(polylineQuery)

    def create_polyline_cartesianPoint_relationship(self):
        SEPARATOR = ','
        polylineQuery = "MATCH (gc:ifcID {label: 'IFCGEOMETRICCURVESET'})-[:DEFINED_BY]->(p:ifcID {label: 'IFCPOLYLINE'}) RETURN p.id, p.attributes;"
        result = self.session.run(polylineQuery)
        for node in result:
            polylineId = node[0]
            attributes = node[1]
            split_cartesianPointIds = attributes.lstrip('(').rstrip(')').split(SEPARATOR)
            cartesianPointIds = SEPARATOR.join("'" + elem + "'" for elem in split_cartesianPointIds)
            cartesianPointQuery = "MATCH (p:ifcID {{id: '{}'}}), (c:ifcID) WHERE c.id IN [{}] MERGE (p)-[:CONTAINS]->(c)".format(
                polylineId, cartesianPointIds)
            self.session.run(cartesianPointQuery)
            print(cartesianPointQuery)
