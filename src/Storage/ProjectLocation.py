from src.Storage.Neo4jHandler import Neo4jHandler


class ProjectLocation(Neo4jHandler):
    def __init__(self):
        super().__init__()
        self.trueNorth = self.trueNorthInfo()
        self.cartPoint = self.insertionCoordSystemPoint()

    def trueNorthInfo(self):
        FIRST_SEPARATOR = '('
        SECOND_SEPARATOR = ','
        LABEL = 'IFCGEOMETRICREPRESENTATIONCONTEXT'
        result = self.select_node_by_label_with_limit(LABEL, 1)
        for node in result:
            attributes = node[2].rsplit(FIRST_SEPARATOR)
            trueNorthId = attributes[-1].split(SECOND_SEPARATOR)[-1]
            trueNorth_query = "MATCH (n:ifcID {{id: '{}'}}) RETURN n.attributes".format(trueNorthId)
            result = [attr[0] for attr in self.session.run(trueNorth_query)]
            return result[0]

    def insertionCoordSystemPoint(self):
        FIRST_SEPARATOR = '('
        SECOND_SEPARATOR = ','
        LABEL = 'IFCGEOMETRICREPRESENTATIONCONTEXT'
        result = self.select_node_by_label_with_limit(LABEL, 1)
        for node in result:
            attributes_kp = node[2].rsplit(FIRST_SEPARATOR)
            AXIS2PLACEMENT3DId = attributes_kp[-1].split(SECOND_SEPARATOR)[-2]
            AXIS2PLACEMENT3DId_query = "MATCH (n:ifcID {{id: '{}'}}) RETURN n.attributes".format(AXIS2PLACEMENT3DId)
            result_axis_placement = [attr[0] for attr in self.session.run(AXIS2PLACEMENT3DId_query)]
            cartPointId = result_axis_placement[0].split(SECOND_SEPARATOR)[0]
            cartPoint_query = "MATCH (n:ifcID {{id: '{}'}}) RETURN n.attributes".format(cartPointId)
            result_cartPoint = [attr[0] for attr in self.session.run(cartPoint_query)]
            cartPoint = result_cartPoint[0]
            return cartPoint
