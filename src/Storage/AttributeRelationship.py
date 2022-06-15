from Storage.Neo4jHandler import Neo4jHandler


class AttributeRelationship(Neo4jHandler):
    def __init__(self):
        super().__init__()

    def create_attribute_relationship(self):
        self.create_between_reldefinesbyproperties_and_space_relationship()
        self.create_between_reldefinesbyproperties_and_propertyset_relationship()
        self.create_between_propertyset_and_propertysinglevalue_relationship()

    def create_between_reldefinesbyproperties_and_space_relationship(self):
        FIRST_SEPARATOR = '('
        SECOND_SEPARATOR = ','
        STRIP_ELEMENT = ')'
        LABEL = 'IFCRELDEFINESBYPROPERTIES'
        result_space = self.select_node_by_label(LABEL)
        for node in result_space:
            attributes_set = self.extract_attribute(node, SECOND_SEPARATOR)

            SPACEId = attributes_set[-2].lstrip(FIRST_SEPARATOR).rstrip(STRIP_ELEMENT).split(SECOND_SEPARATOR)
            SPACE_query_attr = "MATCH (lp:ifcID {{id: '{}'}}), (n:ifcID {{id: '{}'}}) RETURN lp.label".format(
                SPACEId[0], node[0])
            PROPERTYSINGLEVALUEId_query = "MATCH (lp:ifcID {{id: '{}'}}), (n:ifcID {{id: '{}'}}) MERGE (n)-[r:RELATED]->(lp);".format(
                SPACEId[0], node[0])

            Label_check_space = [attr[0] for attr in self.session.run(SPACE_query_attr)]
            IFCSPACE_LABEL = Label_check_space[0]
            if IFCSPACE_LABEL == 'IFCSPACE':
                IFCSPACE_attr = self.session.run(PROPERTYSINGLEVALUEId_query)
                print(IFCSPACE_attr)

    def create_between_reldefinesbyproperties_and_propertyset_relationship(self):
        SECOND_SEPARATOR = ','
        STRIP_ELEMENT = ')'
        PROPERTYSET_query = "MATCH (gc:ifcID {label: 'IFCRELDEFINESBYPROPERTIES'})-[r:RELATED]->(p:ifcID {label: 'IFCSPACE'}) RETURN gc.id, gc.attributes;"
        LABEL = 'IFCPROPERTYSET'
        result = self.session.run(PROPERTYSET_query)
        for node in result:
            id = node[0]
            attributes = node[1].split(SECOND_SEPARATOR)
            PROPERTYSETId = attributes[-1].rstrip(STRIP_ELEMENT)

            PROPERTYSETLABEL_query = "MATCH (lp:ifcID {{id: '{}'}}), (n:ifcID {{id: '{}'}}) RETURN lp.label;".format(
                PROPERTYSETId, node[0])
            PROPERTYSET_query = "MATCH (lp:ifcID {{id: '{}'}}), (n:ifcID {{id: '{}'}}) MERGE (n)-[:RELATING]->(lp) RETURN lp.attributes;".format(
                PROPERTYSETId, node[0])

            PROPERTYSET_check_label = [attr[0] for attr in self.session.run(PROPERTYSETLABEL_query)]
            PROPERTYSET_LABEL = PROPERTYSET_check_label[0]
            if PROPERTYSET_LABEL == LABEL:
                PROPERTYSET_attr = self.session.run(PROPERTYSET_query)
                print(PROPERTYSET_attr)

    def create_between_propertyset_and_propertysinglevalue_relationship(self):
        FIRST_SEPARATOR = '('
        SECOND_SEPARATOR = ','
        STRIP_ELEMENT = ')'
        PROPERTYSET_query = "MATCH (rd:ifcID {label: 'IFCRELDEFINESBYPROPERTIES'})-[rl:RELATING]->(ps:ifcID {label: 'IFCPROPERTYSET'}) RETURN ps.id, ps.attributes;"
        LABEL = 'IFCPROPERTYSINGLEVALUE'
        result = self.session.run(PROPERTYSET_query)
        for node in result:
            id = node[0]
            attributes = node[1].rstrip(STRIP_ELEMENT).split(FIRST_SEPARATOR)
            SINGLEVALUEId = attributes[1].split(SECOND_SEPARATOR)

            for value in SINGLEVALUEId:
                SINGLEVALUELABEL_query = "MATCH (lp:ifcID {{id: '{}'}}), (n:ifcID {{id: '{}'}}) RETURN lp.label;".format(
                    value, node[0])
                SINGLEVALUE_query = "MATCH (lp:ifcID {{id: '{}'}}), (n:ifcID {{id: '{}'}}) MERGE (n)-[:HAS_PROPERTIES]->(lp) RETURN lp.attributes;".format(
                    value, node[0])
                SINGLEVALUE_check_label = [attr[0] for attr in self.session.run(SINGLEVALUELABEL_query)]
                if SINGLEVALUE_check_label:
                    SINGLEVALUE_LABEL = SINGLEVALUE_check_label[0]
                    if SINGLEVALUE_LABEL == LABEL:
                        SINGLEVALUE_attr = self.session.run(SINGLEVALUE_query)
                        print(SINGLEVALUE_attr)
