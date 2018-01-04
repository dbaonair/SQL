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
VALUES (geometry::STGeomFromText('POLYGON ((0 0, 150 0, 150 150, 0 150, 0 0))', 0));  
GO  

DECLARE @geom1 geometry;  
DECLARE @geom2 geometry;  
DECLARE @result geometry;  

SELECT @geom1 = GeomCol1 FROM VenueRow WHERE id = 1;  
SELECT @geom2 = GeomCol1 FROM VenueRow WHERE id = 2;  
SELECT @result = @geom1.STIntersection(@geom2);
SELECT @result.STAsText()

select @geom2.STBoundary().ToString()



DECLARE @g geometry;  
DECLARE @geom2 geometry;  
SET @g = geometry::STGeomFromText('LINESTRING(0 2, 2 0, 4 2)', 0);  
SELECT @geom2 = GeomCol1 FROM VenueRow WHERE id = 2; 
--SET @h = geometry::STGeomFromText('POINT(1 1)', 0);  
SELECT @g.STIntersects(@geom2);  
