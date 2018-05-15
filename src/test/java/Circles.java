import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import crutches.GeoUtils;
import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Circles extends BaseTestClass {

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
        JavaRDD<Tuple2<Long, Point>> points = sc.objectFile("result/HELP/part-*");

        points
//                .filter(t -> t._1 == -1)
                .filter(t -> t._1 == 3)
                .map(t -> t._2)
                .coalesce(8)
                .map(p -> p.getX() + ", " + p.getY())
                .saveAsTextFile("result/Roach");
    }

    @Test
    public void createPoints() throws IOException {
        GeometryFactory factory = new GeometryFactory();
        GeoUtils.cleanDir("result/points");
        sc.parallelize(GeoUtils.randomPoints(new Coordinate(139.7612, 35.6874), 1_000_000, factory))
                .saveAsObjectFile("result/points");
    }

    //##################################################################################################################

    /**
     * (-1,124288)
     * (0,8089)
     * (1,24153)
     * (2,39933)
     * (3,55582)
     * (4,71946)
     * (5,87669)
     * (6,103747)
     * (7,119765)
     * (8,136370)
     * (9,119130)
     * (10,109328)gi
     */
    @Test
    public void count() throws IOException {
        String path = "result/CountByCircle";
        GeoUtils.cleanDir(path);
        JavaRDD<Tuple2<Long, Point>> points = sc.objectFile("result/HELP/part-*");
        points.mapToPair(t -> Tuple2.apply(t._1, 1L)).reduceByKey(Long::sum)
                .coalesce(1).saveAsTextFile(path);
    }

    /**
     * (0,25.748086693406826)
     * (1,25.627128936656987)
     * (2,25.42213736995463)
     * (3,25.27471441981065)
     * (4,25.445692301532226)
     * (5,25.369008556224312)
     * (6,25.402842893776718)
     * (7,25.414922345867794)
     * (8,25.53407010522561)
     * (9,19.958029863723674)
     * (10,16.5715158270007)
     */
    @Test
    public void density() throws IOException {
        String path = "result/Density";
        GeoUtils.cleanDir(path);
        JavaRDD<Tuple2<Long, Point>> points = sc.objectFile("result/HELP/part-*");
        points
                .mapToPair(t -> Tuple2.apply(t._1, 1L)).reduceByKey(Long::sum)
                .filter(t -> t._1 >= 0)
                .mapToPair(t -> {
                    long pointsCount = t._2;
                    double outerRadius = (t._1 + 1) * 10;
                    double innerRadius = t._1 * 10;
                    double area = Math.PI * (outerRadius * outerRadius - innerRadius * innerRadius);
                    return Tuple2.apply(t._1, pointsCount / area);
                })
                .coalesce(1).saveAsTextFile(path);
    }

    /**
     * (0,3.248360321180461)
     * (1,3.2436515144624893)
     * (2,3.235620344443537)
     * (3,3.229804465923242)
     * (4,3.236546467433777)
     * (5,3.2335282934713128)
     * (6,3.2348610927132)
     * (7,3.235336495449295)
     * (8,3.240013642958172)
     * (9,2.9936315617894746)
     * (10,2.807685307459645)
     */

    @Test
    public void proximity() throws IOException {
        String path = "result/Proximity";
        GeoUtils.cleanDir(path);
        JavaRDD<Tuple2<Long, Point>> points = sc.objectFile("result/HELP/part-*");
        points
                .mapToPair(t -> Tuple2.apply(t._1, 1L)).reduceByKey(Long::sum)
                .filter(t -> t._1 >= 0)
                .mapToPair(t -> {
                    long pointsCount = t._2;
                    double outerRadius = (t._1 + 1) * 10;
                    double innerRadius = t._1 * 10;
                    double area = Math.PI * (outerRadius * outerRadius - innerRadius * innerRadius);
                    return Tuple2.apply(t._1, Math.log(pointsCount / area));
                })
                .coalesce(1).saveAsTextFile(path);
    }
}
