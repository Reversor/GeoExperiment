import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import crutches.GeoUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class Spatial extends Assert {
    private GeometryFactory factory;

    @Before
    public void init() {
        factory = new GeometryFactory();
    }

    @Test
    public void circle() {
        Point point = factory.createPoint(new Coordinate(139.7612,35.6874));
//        Point nearestPoint = shapeFactory.pointXY(-88.331482, 32.325124); // 115.27399319083287
        Point nearestPoint = factory.createPoint(new Coordinate(139.76185,35.6866)); // 115.2748159708515

        /*Circle circle = shapeFactory.circle(point, 10);
        double distVincenty = DistanceUtils.radians2Dist(DistanceUtils.distVincentyRAD(
                toRadians(point.getX()), toRadians(point.getY()),
                toRadians(nearestPoint.getX()), toRadians(nearestPoint.getY())), 6356752.314245);
        System.out.println(distVincenty);
        System.out.println(sc.calcDistance(nearestPoint, circle.getCenter()));
        System.out.println(DistanceUtils.distHaversineRAD(point.getX(), point.getY(), nearestPoint.getX(), nearestPoint.getY()));
        double dist = DistanceUtils.degrees2Dist(
                DistanceUtils.distLawOfCosinesRAD(point.getX(), point.getY(), nearestPoint.getX(), nearestPoint.getY()),
                6356752.314245);*/
        System.out.println(GeoUtils.distance(point.getCoordinate(),nearestPoint.getCoordinate()));

    }
}
