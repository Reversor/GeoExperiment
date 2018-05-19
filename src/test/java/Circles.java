import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import crutches.GeoUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.commons.math3.util.FastMath.*;
import static org.apache.commons.math3.util.FastMath.sqrt;

public class Circles extends BaseTestClass {

    @Test
    public void usersCount() throws IOException {
        SQLContext sqlContext = SQLContext.getOrCreate(sc.sc());
        Dataset<Row> poi = sqlContext.read().format("csv").option("header", "false")
                .schema(DataTypes.createStructType(
                        new StructField[]{
                                DataTypes.createStructField("x1", DataTypes.DoubleType, false),
                                DataTypes.createStructField("y1", DataTypes.DoubleType, false)
                        }
                ))
                .load("result/poi/part-*");

        Dataset<Row> signal = sqlContext.read().format("csv").option("header", "false")
                .schema(DataTypes.createStructType(
                        new StructField[]{
                                DataTypes.createStructField("x2", DataTypes.DoubleType, false),
                                DataTypes.createStructField("y2", DataTypes.DoubleType, false),
                                DataTypes.createStructField("user_id", DataTypes.StringType, false)
                        }
                ))
                .load("result/randomed/part-*");

        signal.map((MapFunction<Row, Tuple2<String, Double>>) row -> {
            double x = row.getAs("x1");
            double y = row.getAs("y1");
            double xUser = row.getAs("x2");
            double yUser = row.getAs("y2");
            double distance = 0.0;
            if (!(x == xUser) || !(y == yUser)) {
                double lat1 = y * PI / 180;
                double lon1 = x * PI / 180;
                double lat2 = yUser * PI / 180;
                double lon2 = xUser * PI / 180;

                double hsinX = sin((lon1 - lon2) * 0.5);
                double hsinY = sin((lat1 - lat2) * 0.5);
                double h = hsinY * hsinY + (cos(lat1) * cos(lat2) * hsinX * hsinX);
                if (h > 1) {
                    h = 1;
                }
                distance = 2 * atan2(sqrt(h), sqrt(1 - h)) * 63_781_370;
            }
            String userId = row.getAs("user_id");
            return new Tuple2<>(userId, distance);
        }, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE())).toDF("user_id", "distance");
    }

    @Test
    public void look() throws IOException {
        GeoUtils.cleanDir("result/check");
        JavaRDD<Tuple2<Coordinate, Integer>> points = sc.objectFile("result/shit/part-*");

        points
                .mapToPair(t -> Tuple2.apply(t._2, 1L))
                .reduceByKey(Long::sum)
                .saveAsTextFile("result/check");
//                .saveAsTextFile("result/Roach");
    }

    @Test
    public void user() throws IOException {
        JavaRDD<Point> points = sc.objectFile("result/points/part-*");
        Integer maxUserId = 200;
        GeoUtils.cleanDir("result/gaussian");
        AtomicInteger index = new AtomicInteger(20);
        Random random = new Random();
        points
                .map(p -> new Tuple2<>(p.getCoordinate(),
                        random.nextGaussian() * maxUserId))
                .map(t -> t._1.y + ", " + t._1.x + ", " + Math.round(t._2))
                .coalesce(4)
                .saveAsTextFile("result/gaussian");
    }

    @Test
    public void createPoints() throws IOException {
        GeometryFactory factory = new GeometryFactory();
        GeoUtils.cleanDir("result/poi");

        sc.parallelize(GeoUtils.randomPoints(new Coordinate(139.7612, 35.6874), 1_000, factory))
                .zipWithIndex()
                .map(t -> t._1.getY() + ", " + t._1.getX() + ", " + RandomStringUtils.randomAlphabetic(3) + t._2)
                .coalesce(2)
                .saveAsTextFile("result/poi");

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
        points.mapToPair(t -> Tuple2.apply(t._1, 1L))
                .reduceByKey(Long::sum)
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
