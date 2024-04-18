# sporacle 
Common components for query of spatial data in Oracle database.

# Requirements
oracledb
Geopandas

# Known issues
things go wrong when user does not have access to the table

# Usage Example

```
db = OracleSpatialQueries(
    user=os.environ.get("o_user"), 
    password=os.environ.get("o_pass")
)
datatable = "WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY" 
q = "PROJ_AGE_1 > 30"
db.add_aoi("mytest_aoi.kml")
if db.has_relate(table=datatable, dfn_query=q):
    db.get_intersecting(table=datatable,dfn_query=q)
```

