import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class BaseTestClass extends Assert {
    JavaSparkContext sc;

    @Before
    public void init() {
        SparkConf conf = new SparkConf()
                .setAppName("geo")
                .setMaster("local[*]")
                .set("spark.serializer", KryoSerializer.class.getName())
                .set("spark.kryo.registrator", GeoSparkKryoRegistrator.class.getName());
        sc = new JavaSparkContext(conf);
    }

    @After
    public void end() {
        sc.close();
    }
}
