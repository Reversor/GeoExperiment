import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.operation.distance.DistanceOp;
import crutches.GeoUtils;
import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.junit.Test;
import org.locationtech.spatial4j.distance.DistanceUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.locationtech.spatial4j.distance.DistanceUtils.distLawOfCosinesRAD;

public class Circles extends BaseTestClass {
    @Test
    public void webMercatorCircle() {
        GeometryFactory factory = new GeometryFactory();
        Point center = factory.createPoint(new Coordinate(139.7612, 35.6874));
//        Point notRandomPoint = factory.createPoint(new Coordinate(-88.331482, 32.325124)); // 115.80842264912752
        Point notRandomPoint = factory.createPoint(new Coordinate(139.76185, 35.6866)); // 42.08342151625018

        double radius = 10d;

        Circle circle = new Circle(center, radius);
        Point webMercatorCenter = (Point) GeoUtils.toWebMercator(circle.getCenterGeometry(), "epsg:4326");

        Point webMercatorPoint = GeoUtils.toWebMercator(notRandomPoint, "epsg:4326");

        System.out.println(webMercatorPoint.distance(webMercatorCenter));
        double distance = distLawOfCosinesRAD(
                center.getX(), center.getY(),
                notRandomPoint.getX(), notRandomPoint.getY());
        System.out.println(DistanceUtils.degrees2Dist(distance, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM));

        System.out.println(DistanceUtils.degrees2Dist(center.distance(notRandomPoint), DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM));
        System.out.println(DistanceUtils.degrees2Dist(DistanceOp.distance(center, notRandomPoint), 6356752.314245));
        double webMercatorDistance = distLawOfCosinesRAD(
                webMercatorCenter.getX(), webMercatorCenter.getY(),
                webMercatorPoint.getX(), webMercatorPoint.getY());
        System.out.println(DistanceUtils.degrees2Dist(webMercatorDistance, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM));
    }

    @Test
    public void concentricCircles() throws IOException {
        GeometryFactory factory = new GeometryFactory();
        Point center = factory.createPoint(new Coordinate(139.7612, 35.6874));

        List<Circle> circleList = new ArrayList<>();

        for (int i = 1; i <= 11; i++) {
            circleList.add(new Circle(center, i * 10d));
        }

        String dir = "testResult/circles";
        GeoUtils.cleanDir(dir);
        sc.parallelize(circleList).saveAsObjectFile(dir);
    }

    @Test
    public void look() throws IOException {
        GeoUtils.cleanDir("result/Roach");
        JavaRDD<Tuple2<Long, Point>> points = sc.objectFile("result/HELP/part-00000");
//        JavaRDD<Point> points = sc.objectFile("result/points/part-00000");


        points
                .filter(t -> t._1 == -1)
                .map(t -> t._2)
                .coalesce(8)
                .map(p -> p.getX() + ", " + p.getY())
//                .mapPartitions(GeoUtils::toFeatureCollection)
                .saveAsTextFile("result/Roach");
    }

    @Test
    public void createPoints() throws IOException {
        GeometryFactory factory = new GeometryFactory();
        GeoUtils.cleanDir("result/points");
        sc.parallelize(GeoUtils.randomPoints(new Coordinate(139.7612, 35.6874), 1_000_000, factory))
                .saveAsObjectFile("result/points");
    }
}
