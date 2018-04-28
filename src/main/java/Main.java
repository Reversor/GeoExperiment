import com.vividsolutions.jts.geom.*;
import crutches.GeoUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.util.*;

import static crutches.GeoUtils.*;

public class Main {

    public static void main(String[] args) throws Exception {
        final SparkConf sparkConf = new SparkConf()
                .setAppName("geo")
                .setMaster("local[*]")
                .set("spark.serializer", KryoSerializer.class.getName())
                .set("spark.kryo.registrator", GeoSparkKryoRegistrator.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            String path = "result/";
            PointRDD pointRDD = new PointRDD(
                    sc,
                    "src/main/resources/checkinshape.csv",
                    0,
                    FileDataSplitter.CSV,
                    false);

            // Well
            CircleRDD circleRDD = new CircleRDD(pointRDD, 0.2);


            cleanDir(path);

            double circleRadius = 0.2;
            PolygonRDD circlePolygon = new PolygonRDD(
                    pointRDD.rawSpatialRDD.map(point -> polygonAroundCenter(point, 32, circleRadius))
            );
            double hexagonRadius = circleRadius * 2 / Math.sqrt(3);
            PolygonRDD hexagonPolygon = new PolygonRDD(
                    pointRDD.rawSpatialRDD.map(point -> polygonAroundCenter(point, 6, hexagonRadius))
            );

            circlePolygon.saveAsGeoJSON(path + "circle");
            hexagonPolygon.saveAsGeoJSON(path + "hexagon");
            pointRDD.saveAsGeoJSON(path + "point");

            hexagonPolygon.getRawSpatialRDD()
                    .mapPartitions(GeoUtils::toFeatureCollection)
                    .saveAsTextFile(path + "lol");

            JavaRDD<GeometryCollection> concentricCircles = pointRDD.getRawSpatialRDD().map(point -> {
                int angles = 32;
                double offset = 0.01;
                List<Polygon> polygons = new ArrayList<>();
                for (double i = offset; i < 0.11; i += offset) {
                    polygons.add(polygonAroundCenter(point, angles, i));
                }
                return new GeometryFactory().createGeometryCollection(polygons.toArray(new Polygon[0]));
            });
            //NOPE
            JavaRDD<FeatureCollection> anotherConcentricCircles = pointRDD.getRawSpatialRDD().map(point -> {
                GeoJSONWriter writer = new GeoJSONWriter();
                int angles = 32;
                double offset = 0.01;
                List<Feature> polygons = new ArrayList<>();
                for (double i = offset; i < 0.11; i += offset) {
                    polygons.add(new Feature(writer.write(polygonAroundCenter(point, angles, i)), null));
                }
                return new FeatureCollection(polygons.toArray(new Feature[0]));
            });

            concentricCircles
                    .map(GeoUtils::geometryToFeature).saveAsTextFile(path + "concentricPartial");
            concentricCircles
                    .mapPartitions(GeoUtils::toFeatureCollection).saveAsTextFile(path + "concentricGroup");

        }
    }


}
