import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import crutches.GeoUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PointGeneration extends BaseTestClass {

    @Test
    public void writePoint() throws IOException {
        GeometryFactory factory = new GeometryFactory();
        Coordinate center = new Coordinate(139.7612,35.6874);
        int pointCount = 1_000_000;
        List<Point> pointList = new ArrayList<>(pointCount);
        for (int i = 0; i < pointCount; i++) {
            pointList.add(GeoUtils.randomPointAroundCoordinate(center, 130, factory));
        }
        String dir = "testResult/points";
        GeoUtils.cleanDir("testResult/points");
        sc.parallelize(pointList).saveAsObjectFile(dir);
    }
}
