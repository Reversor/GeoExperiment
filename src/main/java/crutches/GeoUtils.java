package crutches;

import com.vividsolutions.jts.geom.*;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.utils.CRSTransformation;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.lang.Math.PI;
import static java.lang.Math.random;

public class GeoUtils {

    public static Iterator<String> toFeatureCollection(Iterator<? extends Geometry> it) {
        ArrayList<String> result = new ArrayList<>();
        GeoJSONWriter writer = new GeoJSONWriter();
        List<Feature> featureList = new ArrayList<>();
        while (it.hasNext()) {
            Geometry spatialObject = it.next();
            Feature jsonFeature;
            if (spatialObject.getUserData() != null) {
                Map<String, Object> userData = new HashMap<>();
                userData.put("UserData", spatialObject.getUserData());
                jsonFeature = new Feature(writer.write(spatialObject), userData);
            } else {
                jsonFeature = new Feature(writer.write(spatialObject), null);
            }
            featureList.add(jsonFeature);
        }
        FeatureCollection featureCollection = new FeatureCollection(featureList.toArray(new Feature[0]));
        result.add(featureCollection.toString());
        return result.iterator();
    }

    public static Polygon circleToPolygon(Circle circle, GeometryFactory factory) {
        return polygonAroundCenter(circle.getCenterGeometry(), 32, circle.getRadius(), factory);
    }

    public static Point randomPoint(Point p, Double side, GeometryFactory factory) {
        double x = p.getX() + side * Math.random() / 2;
        double y = p.getY() + side * Math.random() / 2;
        return factory.createPoint(new Coordinate(x, y));
    }

    public static Polygon polygonAroundCenter(Geometry geometry, int angles, double radius, GeometryFactory factory) {
        Coordinate center = geometry.getCentroid().getCoordinate();
        Coordinate[] polygonCoordinate = new Coordinate[angles + 1];
        double distanceX = radius / (111320 * Math.cos(center.y * PI / 180));
        double distanceY = radius / 110574;
        for (int i = 0; i <= angles; i++) {
            double theta = ((double) i / angles) * (2 * PI);
            polygonCoordinate[i] = (new Coordinate(
                    center.x + distanceX * Math.cos(theta),
                    center.y + distanceY * Math.sin(theta)
            ));
        }
        Polygon result = factory.createPolygon(polygonCoordinate);
        ;
        polygonCoordinate[angles] = polygonCoordinate[0];
        result.setUserData(geometry.getUserData());
        return result;
    }

    public static Feature geometryToFeature(Geometry geometry) {
        return new Feature(new GeoJSONWriter().write(geometry), null);
    }

    public static Point randomPointAroundCoordinate(Coordinate coordinate, double radius, GeometryFactory factory) {
        double angle = Math.random() * 360 * 2 * PI;
        radius *= Math.random();
        double x = Math.cos(angle) * radius / (111320 * Math.cos(coordinate.y * PI / 180));
        double y = Math.sin(angle) * radius / 110574;
        return factory.createPoint(new Coordinate(coordinate.x + x, coordinate.y + y));
    }

    public static void cleanDir(String strPath) throws IOException {
        Path path = Paths.get(strPath);
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public static <T extends Geometry> T toWebMercator(T geometry, String sourceEspgCRSCode) {
        return CRSTransformation.Transform(sourceEspgCRSCode, "epsg:3785", geometry);
    }

    public static <T extends Geometry> T toWGS84(T geometry, String sourceEspgCRSCode) {
        System.out.println(geometry.getClass());
        return CRSTransformation.Transform(sourceEspgCRSCode, "epsg:4326", geometry);
    }

    public static List<Point> randomPoints(Coordinate c, int count, GeometryFactory factory) {
        double offset = DistanceUtils.dist2Degrees(110, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM * 1000);
        List<Point> pointList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            double x = c.x - offset + 2 * offset * random();
            double y = c.y - offset + 2 * offset * random();
            pointList.add(factory.createPoint(new Coordinate(x, y)));
        }
        return pointList;
    }

    public static double uglyDistance(Coordinate c1, Coordinate c2) {
        if (c1.x == c2.x && c1.y == c2.y) {
            return 0.0;
        }

        double lat1 = c1.x * PI / 180;
        double lon1 = c1.y * PI / 180;
        double lat2 = c2.x * PI / 180;
        double lon2 = c2.y * PI / 180;

        double cosLat1 = Math.cos(lat1);
        double sinLat1 = Math.sin(lat1);
        double cosLat2 = Math.cos(lat2);
        double sinLat2 = Math.sin(lat2);
        double dLon = lon2 - lon1;
        double cosDLon = Math.cos(dLon);
        double sinDLon = Math.sin(dLon);

        double a = cosLat2 * sinDLon;
        double b = cosLat1 * sinLat2 - sinLat1 * cosLat2 * cosDLon;
        double c = sinLat1 * sinLat2 + cosLat1 * cosLat2 * cosDLon;

        return Math.atan2(Math.sqrt(a * a + b * b), c) * DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM * 1000;
    }

    public static double anotherDistance(Coordinate c1, Coordinate c2) {
        if (c1.x == c2.x && c1.y == c2.y) {
            return 0.0;
        }

        double lat1 = c1.y * PI / 180;
        double lon1 = c1.x * PI / 180;
        double lat2 = c2.y * PI / 180;
        double lon2 = c2.x * PI / 180;

        double hsinX = Math.sin((lon1 - lon2) * 0.5);
        double hsinY = Math.sin((lat1 - lat2) * 0.5);
        double h = hsinY * hsinY + (Math.cos(lat1) * Math.cos(lat2) * hsinX * hsinX);
        if (h > 1) {
            h = 1;
        }
        return 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h)) * DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM * 1000;
    }

    public static double distance(Coordinate c1, Coordinate c2) {
        return Geodesic.WGS84.Inverse(c1.y, c1.x, c2.y, c2.x, GeodesicMask.DISTANCE).s12;
    }
}
