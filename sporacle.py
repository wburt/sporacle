import os
import oracledb
import geopandas as gpd
import logging
import fiona

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)
c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)
logger.addHandler(c_handler)

fiona.drvsupport.supported_drivers["libkml"] = "rw"

class Aoi:
    """AOI import and reprojection within preset coordinate system
    with methods to return WKT, WKB, and buffers"""

    EPSG = 3005

    def __init__(self, input) -> None:
        fiona.drvsupport.supported_drivers["LIBKML"] = "rw"
        df = gpd.read_file(input)
        if self.EPSG == df.crs.to_epsg():
            self.df = df
        else:
            self.df = df.to_crs(f"EPSG:{self.EPSG}")
        self.buffers = {}

    def get_wkt_geom(self):
        wkt = self.df["geometry"].to_wkt().iloc[0]
        return wkt

    def get_wkb_geom(self):
        wkb = self.df["geometry"].to_wkb().iloc[0]
        return wkb
    def get_outisde_buffer(self, distance):
        ''' buffer the aoi by distance(metres)
            caches buffer for reuse 
            see shapely object.buffer for **kwargs'''
        if distance in self.buffers.keys():
            b = self.buffers[distance]
        else:
            # get the exterior buffer
            b1 = self.df.exterior.buffer(distance,single_sided=True)
            logger.debug(f'Left hand buffer area: {b1.area} m2')
            b2 = self.df.exterior.buffer(distance*-1,single_sided=True)
            logger.debug(f'Right hand buffer area: {b2.area} m2')

            if float(b1.area) > float(b2.area):
                self.buffers[distance] = b1
                b = b1
            else:
                self.buffers[distance] = b2
                b = b2
        return b

class OracleSpatialQueries:
    """
    OracleSpatialQueries provides common components for query against spatial data stored in Oracle
    
    """

    HOST = "bcgw.bcgov"
    DATABASE = "idwprod1.bcgov"
    PORT = 1521

    def __init__(self, user, password) -> None:
        """
        Initialize with Oracle connection
        """
        self.aoi = None
        self.conn = None
        self.connect(user=user, password=password)
        oracledb.defaults.fetch_lobs = False
        self.table_dict = {}  # future cache for data schemas? {"name":{"geom_column":"SHAPE","geom_type":"POLYGON"}}

    def connect(self, user, password):
        """
        Connect to oracle
        """
        if self.conn is None:
            try:
                self.conn = oracledb.connect(
                    user=user,
                    password=password,
                    host=self.HOST,
                    port=self.PORT,
                    service_name=self.DATABASE,
                )
            except Exception as e:
                print(e.message)
        logger.debug(self.conn.is_healthy())

    def add_aoi(self, aoi):
        """ 
        Construct Aoi from file
        filetype must be supported by Geopandas
        """
        self.aoi = Aoi(aoi)

    def has_relate(self, table, dfn_query=None,buffer=None):
        """
        Checks for features from input table that are spatially related to aoi
        returns true if related geometry records exist
        buffer: buffer distance in metres, if this is indicated 
            only the ring buffer of this distance is used
        """
        if self.has_table(db_table=table) is False:
            raise Exception(f"Table {table} does not exist for this user")
        logger.debug("Get related from {table}")
        geom_column = self.get_bcgw_geomcolumn(db_table=table)

        with self.conn.cursor() as cursor:
            cursor.setinputsizes(wkb=oracledb.DB_TYPE_BLOB)
            # query for at least one related record
            query = f"SELECT ROWNUM FROM {table} WHERE SDO_RELATE ({geom_column}, SDO_GEOMETRY(:wkb,:srid),'mask=ANYINTERACT') = 'TRUE' and ROWNUM=1"
            if dfn_query is not None:
                query = query + f" AND {dfn_query}"
            if buffer is None:
                wkb = self.aoi.get_wkb_geom()
            else:
                wkb = self.aoi.get_outisde_buffer(buffer).to_wkb().iloc[0]
            params = {"wkb": wkb, "srid": self.aoi.EPSG}
            cursor.execute(query, params)
            row = cursor.fetchone()
            if row is not None:
                logger.debug(f"{table} has features overlaping with AOI")
        if row is not None:
            return True
        else:
            return False

    def get_related(self, table, dfn_query=None,buffer=None):
        """
        Gets features from input table that are spatially related to aoi
        returns geopandas geodataframe
        buffer: buffer distance in metres, if this is indicated only the ring buffer of this distance is used
        """
        if self.has_table(db_table=table) is False:
            raise Exception(f"Table {table} does not exist for this user")
        logger.debug(f"Get related from {table}")
        geom_column = self.get_bcgw_geomcolumn(db_table=table)
        all_columns = self.get_bcgw_columns(db_table=table)
        all_columns.remove(geom_column)
        columns_str = ",".join(all_columns)
        with self.conn.cursor() as cursor:
            cursor.setinputsizes(wkb=oracledb.DB_TYPE_BLOB)
            # query for related records
            query = f"SELECT {columns_str},sdo_util.to_wkbgeometry({geom_column}) wkb_geom FROM {table} b WHERE SDO_RELATE (b.{geom_column}, SDO_GEOMETRY(:wkb,:srid),'mask=ANYINTERACT') = 'TRUE'"
            if dfn_query is not None:
                query = query + f" AND {dfn_query}"
            if buffer is None:
                wkb = self.aoi.get_wkb_geom()
            else:
                wkb = self.aoi.get_outisde_buffer(buffer).to_wkb().iloc[0]
            params = {"wkb": wkb, "srid": self.aoi.EPSG}
            cursor.execute(query, params)
            rows = cursor.fetchall()
            all_columns.append("wkb_geom")  # append name of wkbgeom column

        if rows is not None:
            logger.debug(f"{table} has {len(rows)} features overlaping with AOI")
            gdf = gpd.GeoDataFrame(rows, columns=all_columns)
            # gdf["geom"] = gpd.GeoSeries(gdf["wkb_geom"].apply(lambda x: loads(x)))
            gdf["geom"] = gpd.GeoSeries.from_wkb(
                data=gdf["wkb_geom"], crs=f"EPSG:{self.aoi.EPSG}"
            )
            gdf.set_geometry(col="geom", inplace=True, crs=f"EPSG:{self.aoi.EPSG}")
        else:
            gdf = None
        return gdf

    def get_intersecting(self, table, dfn_query=None,buffer=None):
        """
        Gets intersection features from input table that are spatially related to aoi
        returns geopandas geodataframe
        buffer: buffer distance in metres, if this is indicated only the ring buffer of this distance is intersected
        """
        if self.has_table(db_table=table) is False:
            raise Exception(f"Table {table} does not exist for this user")
        logger.debug(f"Get intersection of {table}")
        geom_column = self.get_bcgw_geomcolumn(db_table=table)
        all_columns = self.get_bcgw_columns(db_table=table)
        all_columns.remove(geom_column)
        columns_str = ",".join(all_columns)
        if dfn_query:
            q = f"AND {dfn_query}"
        with self.conn.cursor() as cursor:
            cursor.setinputsizes(wkb=oracledb.DB_TYPE_BLOB)
            # select intersecting records
            query = f"""
            SELECT 
                {columns_str},sdo_util.to_wkbgeometry(sdo_geom.sdo_intersection({geom_column},  
                SDO_GEOMETRY(:wkb,:srid), 1)) wkb_geom 
            FROM {table} 
            WHERE SDO_RELATE ({geom_column}, SDO_GEOMETRY(:wkb,:srid),'mask=ANYINTERACT') = 'TRUE'
            """
            if dfn_query:
                query = query + q
            if buffer is None:
                wkb = self.aoi.get_wkb_geom()
            else:
                wkb = self.aoi.get_outisde_buffer(buffer).to_wkb().iloc[0]
            params = {"wkb": wkb, "srid": self.aoi.EPSG}
            cursor.execute(query, params)
            rows = cursor.fetchall()
            all_columns.append("wkb_geom")  # append name of wkbgeom column
        if rows is not None:
            logger.debug(f"{table} has {len(rows)} features overlaping with AOI")
            gdf = gpd.GeoDataFrame(rows, columns=all_columns)
            gdf["geom"] = gpd.GeoSeries.from_wkb(
                data=gdf["wkb_geom"], crs=f"EPSG:{self.aoi.EPSG}"
            )
            gdf.set_geometry(col="geom", inplace=True, crs=f"EPSG:{self.aoi.EPSG}")
        else:
            gdf = None
        return gdf

    def get_intersect_local(self, table, dfn_query=None,buffer=None):
        if self.has_relate(table=table, dfn_query=dfn_query):
            df1 = self.get_related(table=table, dfn_query=dfn_query, buffer=buffer)
            if buffer is None:
                intersection_df = df1.overlay(right=self.aoi.df, how="intersection")
            else:
                the_aoi = gpd.GeoDataFrame(self.aoi.get_outisde_buffer(buffer))
                the_aoi = the_aoi.rename(columns={0:'geometry'}).set_geometry('geometry',crs=f"EPSG:{self.aoi.EPSG}")
                intersection_df = df1.overlay(right=the_aoi, how="intersection")
            return intersection_df
        else:
            return None

    def has_table(self, db_table):
        """
        Checks if table exists for current users privilege
        returns Boolean
        """
        # check if table has been checked before
        if db_table in self.table_dict.keys():
            return True
        owner, table = db_table.split(".")
        query = f"""
            SELECT SUM(OBJ_CNT) from (
            SELECT count(ROWNUM) obj_cnt FROM all_views where owner = '{owner}' and view_name = '{table}' UNION
            SELECT count(ROWNUM) obj_cnt FROM all_tables where owner = '{owner}' and table_name = '{table}')
            """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            logger.debug(f"has_table result is {result}")
        if result[0] > 0:
            self.table_dict[db_table]={}
            return True
        else:
            return False

    def get_bcgw_geomcolumn(self, db_table):
        """returns the name of the geometry column for oracle table"""
        if self.has_table(db_table=db_table) is False:
            raise Exception(f"Table {table} does not exist for this user")
        owner, table = db_table.split(".")
        query = f"SELECT COLUMN_NAME from all_tab_columns where OWNER = '{owner}' \
            AND TABLE_NAME = '{table}' AND DATA_TYPE = 'SDO_GEOMETRY'"
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            q = cursor.fetchone()
            geom_c = q[0]
        return geom_c

    def get_bcgw_columns(self, db_table):
        """returns the names of the columns for oracle table"""
        if self.has_table(db_table=db_table) is False:
            raise Exception(f"Table {table} does not exist for this user")
        owner, table = db_table.split(".")
        query = f"""
            SELECT COLUMN_NAME from all_tab_columns 
            where OWNER = :owner
                AND TABLE_NAME = '{table}'
                AND COLUMN_NAME<>'SE_ANNO_CAD_DATA'"""
        params = {"owner": owner}
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [c[0] for c in rows]
            logging.debug(f"get_bcgw_columns --> {','.join(columns)}")
        return columns

    def __del__(self):
        """
        Deconstruct: close the connection
        """
        if self.conn is not None:
            self.conn.close()


if __name__ == "__main__":
    # Example usage
    db = OracleSpatialQueries(
        user=os.environ.get("o_user"), password=os.environ.get("o_pass")
    )
    datatable = "WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY" 
    q = "PROJ_AGE_1 > 30"
    db.add_aoi("mytest_aoi.kml")
    if db.has_relate(table=datatable, dfn_query=q):
        db.get_intersecting(table=datatable,dfn_query=q)

