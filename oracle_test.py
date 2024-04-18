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
    """API import and conversion within preset coordinate system"""

    EPSG = 3005

    def __init__(self, input) -> None:
        df = gpd.read_file(input)
        if self.EPSG == df.crs.to_epsg():
            self.df = df
        else:
            self.df = df.to_crs(f"EPSG:{self.EPSG}")

    def get_wkt_geom(self):
        wkt = self.df["geometry"].to_wkt().iloc[0]
        return wkt

    def get_wkb_geom(self):
        wkb = self.df["geometry"].to_wkb().iloc[0]
        return wkb


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

    def has_relate(self, table, dfn_query=None):
        """
        Checks for features from input table that are spatially related to aoi
        returns true if related geometry records exist
        """
        logger.debug("Get related from {table}")
        geom_column = self.get_bcgw_geomcolumn(db_table=table)

        with self.conn.cursor() as cursor:
            cursor.setinputsizes(wkb=oracledb.DB_TYPE_BLOB)
            # query for at least one related record
            query = f"SELECT ROWNUM FROM {table} WHERE SDO_RELATE ({geom_column}, SDO_GEOMETRY(:wkb,:srid),'mask=ANYINTERACT') = 'TRUE' and ROWNUM=1"
            if dfn_query is not None:
                query = query + f" AND {dfn_query}"
            wkb = self.aoi.get_wkb_geom()
            params = {"wkb": wkb, "srid": self.aoi.EPSG}
            cursor.execute(query, params)
            row = cursor.fetchone()
            if row[0] > 0:
                logger.debug(f"{table} has features overlaping with AOI")
        if row[0] > 0:
            return True
        else:
            return False

    def get_related(self, table, dfn_query=None):
        """
        Gets features from input table that are spatially related to aoi
        returns geopandas geodataframe
        """
        logger.debug("Get related from {table}")
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
            wkb = self.aoi.get_wkb_geom()
            params = {"wkb": wkb, "srid": self.aoi.EPSG}
            cursor.execute(query, params)
            rows = cursor.fetchall()
            all_columns.append("wkb_geom")  # append name of wkbgeom column

        if len(rows) > 0:
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

    def get_intersecting(self, table, dfn_query=None):
        """
        Gets intersection features from input table that are spatially related to aoi
        returns geopandas geodataframe
        """
        logger.debug("Get related from {table}")
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
            wkb = self.aoi.get_wkb_geom()
            params = {"wkb": wkb, "srid": self.aoi.EPSG}
            cursor.execute(query, params)
            rows = cursor.fetchall()
            logger.debug(f"{table} has {len(rows)} features overlaping with AOI")
            all_columns.append("wkb_geom")  # append name of wkbgeom column
        if len(rows) > 0:
            gdf = gpd.GeoDataFrame(rows, columns=all_columns)
            gdf["geom"] = gpd.GeoSeries.from_wkb(
                data=gdf["wkb_geom"], crs=f"EPSG:{self.aoi.EPSG}"
            )
            gdf.set_geometry(col="geom", inplace=True, crs=f"EPSG:{self.aoi.EPSG}")
        else:
            gdf = None
        return gdf

    def get_intersect_local(self, table, dfn_query=None):
        if self.has_relate(table=table, dfn_query=dfn_query):
            df1 = self.get_related(table=table, dfn_query=dfn_query)
            intersection_df = df1.overlay(right=self.aoi.df, how="intersection")
            return intersection_df
        else:
            return None

    def set_aoi_buffer(self, buffer):
        pass

    def get_bcgw_geomcolumn(self, db_table):
        """returns the name of the geometry column for oracle table"""
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

