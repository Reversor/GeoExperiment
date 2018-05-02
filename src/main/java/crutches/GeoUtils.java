package crutches;

import com.vividsolutions.jts.geom.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

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

    public static Polygon polygonAroundCenter(Geometry geometry, int angles, double radius, GeometryFactory factory) {
        Coordinate center = geometry.getCentroid().getCoordinate();
        Coordinate[] polygonCoordinate = new Coordinate[angles + 1];
        double distanceX = radius / (111.320 * Math.cos(center.y * Math.PI / 180));
        double distanceY = radius / 110.574;
        for (int i = 0; i <= angles; i++) {
            double theta = ((double) i / angles) * (2 * Math.PI);
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
        double randomDouble = Math.random();
        double angle = randomDouble * 2 * Math.PI;
        double x = randomDouble * Math.cos(angle) * radius / (111.320 * Math.cos(coordinate.y * Math.PI / 180));
        double y = randomDouble * Math.sin(angle) * radius / 110.574;
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

}
