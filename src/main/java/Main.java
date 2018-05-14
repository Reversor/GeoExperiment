import crutches.GeoUtils;
import crutches.SerializableComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import scala.Tuple2;

import java.util.List;
import java.util.Optional;

import static crutches.GeoUtils.uglyDistance;

public class Main {

    public static void main(String[] args) throws Exception {
        long time;
        GeoUtils.cleanDir("result/HELP");

        final SparkConf sparkConf = new SparkConf()
                .setAppName("geo")
                .setMaster("local[*]")
                .set("spark.serializer", KryoSerializer.class.getName())
                .set("spark.kryo.registrator", GeoSparkKryoRegistrator.class.getName());
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            time = System.currentTimeMillis();
            JavaRDD<Circle> circleRDD = sc.objectFile("testResult/circles/part-*");
            JavaPairRDD<Circle, Long> indexedCircles = circleRDD.sortBy(Circle::getRadius, true, 1).zipWithIndex();
            List<Tuple2<Circle, Long>> circles = indexedCircles.collect();

            PointRDD pointRDD = new PointRDD(sc.objectFile("testResult/points/part-*"));

            pointRDD.getRawSpatialRDD().mapToPair(point -> {
                Optional<Tuple2<Circle, Long>> nearest = circles.stream()
                        .filter(c -> uglyDistance(c._1.getCenterPoint(), point.getCoordinate()) < c._1.getRadius())
                        .min(SerializableComparator.serialize(
                                (c1, c2) -> c1._1.getRadius().compareTo(c2._1.getRadius())
                        ));
                return nearest
                        .map(circleLongTuple2 -> new Tuple2<>(circleLongTuple2._2, point))
                        .orElseGet(() -> new Tuple2<>(-1L, point));
            }).saveAsObjectFile("result/HELP");
        }
        time -= System.currentTimeMillis();
        System.out.println(-time);
    }
}
