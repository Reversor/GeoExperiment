import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import crutches.GeoUtils;
import crutches.SerializableComparator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CirclesAndPoints extends BaseTestClass {

    @Test
    public void testKNN() throws IOException {
        GeometryFactory factory = new GeometryFactory();
        Point center = factory.createPoint(new Coordinate(139.7612, 35.6874));

        List<Circle> circleList = new ArrayList<>();

        for (int i = 1; i <= 11; i++) {
            circleList.add(new Circle(center, i * 10d));
        }

        CircleRDD circleRDD = new CircleRDD(sc.parallelize(circleList));
        PointRDD pointRDD = new PointRDD(sc.objectFile("testResult/points"));

        Point randomPoint = GeoUtils.randomPointAroundCoordinate(center.getCoordinate(), 140d, factory);

        JavaPairRDD<Circle, Long> circleWithIndex = circleRDD.getRawSpatialRDD()
                .sortBy(Circle::getRadius, true, 1)
                .zipWithIndex()
                .filter(c -> GeoUtils.uglyDistance(c._1.getCenterPoint(), randomPoint.getCoordinate())
                        < c._1.getRadius())
                .persist(StorageLevel.MEMORY_ONLY());
        Tuple2<Circle, Long> circle;
        if (circleWithIndex.count() != 0) {
            circle = circleWithIndex
                    .min(
                            SerializableComparator.serialize(
                                    (c1, c2) -> c1._1.getRadius().compareTo(c2._1.getRadius())
                            )
                    );
        } else {
            circle = new Tuple2<>(new Circle(center, 0d), 9999L);
        }


        Polygon polygon = GeoUtils.circleToPolygon(circle._1, factory);
        polygon.setUserData(circle._2);
        String dir = "testResult/dich";
        GeoUtils.cleanDir(dir);
        sc.parallelize(Arrays.asList(polygon, randomPoint))
                .coalesce(1).mapPartitions(GeoUtils::toFeatureCollection).saveAsTextFile(dir);

    }

}
