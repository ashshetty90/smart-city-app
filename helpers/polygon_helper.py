import geopandas as gpd
from shapely import wkt


class PolygonHelper:
    """
    Helper class to validate if a lat,long lie inside a provided polygon
    """
    def __init__(self):
        pass

    @staticmethod
    def pnt_in_ply(
            pnt_df,
            ply_df,
            how="left",
            lat_col=None,
            lng_col=None,
            ply_col=None,
            buffer=None,
    ):
        """Identify points in polygons.
    ​
        lat/lng/ply columns will be determined if not specified.
    ​
        Parameters
        ----------
        pnt_df : pd.DataFrame
            DataFrame containing lat/lng columns.
        ply_df : pd.DataFrame
            DataFrame containing wkt polygon column.
        how : str
            How to match: left, inner or right. See gpd.sjoin for master
            list.
        lat_col : str, latitude column
        lng_col : str, longitude column
        ply_col : str, polygon column
        buffer : float, optional
            Degrees by which to buffer the ply data.
    ​
        """
        ply_gdf = gpd.GeoDataFrame(
            ply_df, geometry=ply_df[ply_col].apply(wkt.loads)
        )
        if buffer is not None:
            ply_gdf["geometry"] = ply_gdf.buffer(buffer, resolution=5)
        pnt_gdf = gpd.GeoDataFrame(
            pnt_df, geometry=gpd.points_from_xy(pnt_df[lng_col], pnt_df[lat_col])
        )

        return gpd.sjoin(pnt_gdf, ply_gdf, how="inner", op="within")
