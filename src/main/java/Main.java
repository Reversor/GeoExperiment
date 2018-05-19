import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.collection.immutable.Range;

import java.io.File;

import static org.apache.commons.math3.util.FastMath.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.floor;


public class Main {

    public static void main(String[] args) throws Exception {
        //можем парсить из аргументов
        int maxRadius = 300;
        int delta = 15;

        assert maxRadius % delta == 0 : "Delta should be a multiple of the maximum radius";

        final int EARTH_RADIUS_M = 63_781_370;
        final double RADIANS_IN_DEGREES = 0.017453292519943295;

        int circlesCount = maxRadius / delta;

        Range baseResult = new Range(0, circlesCount, 1);

        String resultDirectory = "result/csv";

        FileUtils.deleteDirectory(new File(resultDirectory));

        final SparkConf sparkConf = new SparkConf()
                .setAppName("geo")
                .setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            SQLContext sqlContext = SQLContext.getOrCreate(sc.sc());
            sqlContext.setConf(SQLConf.SHUFFLE_PARTITIONS(), 8);

            sqlContext.udf().register("Distance",
                    (UDF4<Double, Double, Double, Double, Double>) (lat1, lon1, lat2, lon2) -> {
                        double result = 0;
                        if (!(lat1.equals(lat2)) || !(lon1.equals(lon2))) {
                            lat1 *= RADIANS_IN_DEGREES;
                            lon1 *= RADIANS_IN_DEGREES;
                            lat2 *= RADIANS_IN_DEGREES;
                            lon2 *= RADIANS_IN_DEGREES;
                            double hsinX = sin((lon1 - lon2) * 0.5);
                            double hsinY = sin((lat1 - lat2) * 0.5);
                            double h = hsinY * hsinY + (cos(lat1) * cos(lat2) * hsinX * hsinX);
                            if (h > 1) {
                                h = 1;
                            }
                            result = 2 * atan2(sqrt(h), sqrt(1 - h)) * EARTH_RADIUS_M;
                        }
                        return result;
                    }, DataTypes.DoubleType);

            Dataset<Row> poi = sqlContext.read().format("csv").option("header", false)
                    .schema(DataTypes.createStructType(
                            new StructField[]{
                                    DataTypes.createStructField("y1", DataTypes.DoubleType, false),
                                    DataTypes.createStructField("x1", DataTypes.DoubleType, false)
                            }
                    ))
                    .load("result/poi/part-*").cache();
            sc.broadcast(poi);

            Dataset<Row> signal = sqlContext.read().format("csv").option("header", false)
                    .schema(DataTypes.createStructType(
                            new StructField[]{
                                    DataTypes.createStructField("y2", DataTypes.DoubleType, false),
                                    DataTypes.createStructField("x2", DataTypes.DoubleType, false),
                                    DataTypes.createStructField("user_id", DataTypes.StringType, false)
                            }
                    ))
                    .load("result/randomed/part-*");

            Dataset<Row> signalPoi = signal.crossJoin(poi)
                    .selectExpr("user_id", "Distance(y1, x1, y2, x2) as distance");

            Dataset<Row> filtered = signalPoi.where(col("distance").leq(maxRadius))
                    .withColumn("circle_id", floor(col("distance").divide(delta)));

            filtered.groupBy("user_id").pivot("circle_id", baseResult).count()
                    .coalesce(1).write().csv(resultDirectory);

        }
    }
}
