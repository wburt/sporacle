# sporacle 
Common components for query of spatial data in Oracle database. Some testing has been done using kml area of interest inputs. 
More testing is needed to see if intersects are faster on client(local) or on db. For now both methods exist.  
### Examples: iterating intersect of different aoi kml with 44 input layers  
| test | Database | Geopandas |
| ---- | -------- | --------- |
| 1    | 78 s     | 86 s |
| 2    |83 s    | 73 s |


# Requirements
oracledb
Geopandas

# Known issues
There are no known issues but also very little testing has been done.

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

