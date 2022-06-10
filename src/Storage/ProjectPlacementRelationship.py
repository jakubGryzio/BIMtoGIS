from src.Storage.Neo4jHandler import Neo4jHandler


class ProjectPlacementRelationship(Neo4jHandler):
    def __init__(self):
        super().__init__()

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
