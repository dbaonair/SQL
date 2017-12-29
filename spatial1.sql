/* create geometry  http://www.opengeospatial.org/standards/sfs  */

CREATE TABLE VenueRow   
    ( id int IDENTITY (1,1),  
    GeomCol1 geometry,   
    GeomCol2 AS GeomCol1.STAsText() );  
GO  

DECLARE @g geometry;  
SET @g = geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0);  
SELECT @g.ToString();  

INSERT INTO VenueRow (GeomCol1)  
VALUES (geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0)); 

DECLARE @geom1 geometry;  
DECLARE @geom2 geometry;  
DECLARE @result geometry;  

SELECT @geom1 = GeomCol1 FROM VenueRow WHERE id = 1;  
SELECT @geom2 = GeomCol1 FROM VenueRow WHERE id = 2;  
SELECT @result = @geom1.STIntersection(@geom2);
SELECT @result.STAsText()

select @geom2.STBoundary().ToString()




SELECT '[' + sc.name +'].' +o.name, SUM(reserved_page_count) * 8.0 / 1024,min(type_desc)
FROM sys.dm_db_partition_stats, sys.objects o,sys.schemas sc 
WHERE sys.dm_db_partition_stats.object_id = o.object_id
and o.schema_id=sc.schema_id
 GROUP BY o.name,sc.name
order by 2 desc;

/* create geohraphy */ 


