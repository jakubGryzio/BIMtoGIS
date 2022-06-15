import numpy as np
from Storage.Neo4jHandler import Neo4jHandler
from Storage.ProjectLocation import ProjectLocation
import geopandas as gpd
import pandas as pd
import shapely.geometry
import warnings
from shapely.errors import ShapelyDeprecationWarning

warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)


class SpatialQuery(Neo4jHandler):
    def __init__(self):
        super().__init__()
        self.location = ProjectLocation()

    def coordinates_query(self, ifcSpaceId):
        coords_query = "MATCH (space:ifcID {{label: 'IFCSPACE', id:'{}'}})-[*4]->(polyline:ifcID {{label: 'IFCPOLYLINE'}})-[:CONTAINS]->(c:ifcID) RETURN c.attributes;".format(
            ifcSpaceId)
        coords_result = self.session.run(coords_query)
        return [coord[0] for coord in coords_result]

    def placement_query(self, ifcSpaceId):
        placement_query = "MATCH (space:ifcID {{label: 'IFCSPACE', id:'{}'}})-[*]->(local:ifcID {{label: 'IFCLOCALPLACEMENT'}})-[*2]->(cartesian:ifcID {{label: 'IFCCARTESIANPOINT'}}) RETURN cartesian.attributes;".format(
            ifcSpaceId)
        placement_result = self.session.run(placement_query)
        return [placement[0] for placement in placement_result]

    def converted_coords(self):
        converted_coords = []
        LABEL = 'IFCSPACE'
        ifcSpace_result = self.select_node_by_label(LABEL)
        for node in ifcSpace_result:
            ifcSpaceId = node[0]
            coordinates = self.coordinates_query(ifcSpaceId)
            placement = self.placement_query(ifcSpaceId)
            deltaX = sum([float(elem.lstrip('(').rstrip(')').split(',')[0]) for elem in placement])
            deltaY = sum([float(elem.lstrip('(').rstrip(')').split(',')[1]) for elem in placement])
            align_coords = []
            for point in coordinates:
                point = point.lstrip('(').rstrip(')').split(',')
                cast_point = [float(coord) for coord in point]
                converted_point = self.calculate_point_coordinate(cast_point, deltaX, deltaY)
                align_coords.append(tuple(converted_point))
            converted_coords.append(align_coords)
        return converted_coords

    def calculate_point_coordinate(self, point, deltaX, deltaY):
        point[0] += deltaX
        point[0] *= 0.00001
        point[0] += self.location.insertionPointWGS[0]
        point[1] += deltaY
        point[1] *= 0.00001
        point[1] += self.location.insertionPointWGS[1]
        return point

    def create_geoDataFrame(self):
        coordinates = self.converted_coords()
        pre_dfs = []
        for polygon in coordinates:
            geometry = shapely.geometry.Polygon(polygon)
            print(geometry)
            geometry = [geometry]
            df = pd.DataFrame({'geometry': geometry})
            pre_dfs.append(df)
        df = pd.concat(pre_dfs, ignore_index=True).reset_index(drop=True)
        return gpd.GeoDataFrame(df,
                                  geometry='geometry',
                                  crs='epsg:4326')

    def upload_shp_file(self):
        geo_df = self.create_geoDataFrame()
        geo_df.to_file('IFCSPACE.shp')
