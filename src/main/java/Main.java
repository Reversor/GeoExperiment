import com.vividsolutions.jts.geom.*;
import crutches.Crutch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.geotools.geometry.jts.GeometryBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static crutches.Crutch.*;

public class Main {

    public static void main(String[] args) throws Exception {
        final SparkConf sparkConf = new SparkConf()
                .setAppName("geo")
                .setMaster("local[*]")
                .set("spark.serializer", KryoSerializer.class.getName())
                .set("spark.kryo.registrator", GeoSparkKryoRegistrator.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            PointRDD pointRDD = new PointRDD(
                    sc,
                    "src/main/resources/checkinshape.csv",
                    0,
                    FileDataSplitter.CSV,
                    false);
            CircleRDD circleRDD = new CircleRDD(pointRDD, 200d);

            String path = "result/";
            cleanDir(path);
            double circleRadius = 0.2;
            PolygonRDD circlePolygon = new PolygonRDD(
                    pointRDD.rawSpatialRDD.map(point -> polygonAroundPoint(point, 32, circleRadius))
            );
            double hexagonRadius = circleRadius * 2 / Math.sqrt(3);
            PolygonRDD hexagonPolygon = new PolygonRDD(
                    pointRDD.rawSpatialRDD.map(point -> polygonAroundPoint(point, 6, hexagonRadius))
            );

            circlePolygon.saveAsGeoJSON(path + "circle");
            hexagonPolygon.saveAsGeoJSON(path + "hexagon");
            pointRDD.saveAsGeoJSON(path + "point");

            hexagonPolygon.getRawSpatialRDD()
                    .mapPartitions(Crutch::toFutureCollection)
                    .saveAsTextFile(path + "lol");

            JavaRDD<Polygon> concentricCircles = pointRDD.getRawSpatialRDD().flatMap(point -> {
                int angles = 32;
                double offset = 0.01;
                List<Polygon> polygons = new ArrayList<>();
                for (double i = offset; i < 0.11; i += offset) {
                    polygons.add(polygonAroundPoint(point, angles, i));
                }
                return polygons.iterator();
            });
            new PolygonRDD(concentricCircles).saveAsGeoJSON(path + "wutface");

        }
    }


}
