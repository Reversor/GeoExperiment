import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.ShapeFactory;

import static org.locationtech.spatial4j.distance.DistanceUtils.toRadians;

public class Spatial extends Assert {
    private SpatialContext sc;
    private ShapeFactory shapeFactory;

    @Before
    public void init() {
        SpatialContextFactory scf = new SpatialContextFactory();
        scf.geo = true;
        sc = scf.newSpatialContext();
        shapeFactory = sc.getShapeFactory();
    }

    @Test
    public void circle() {
        Point point = shapeFactory.pointXY(139.7612,35.6874);
//        Point nearestPoint = shapeFactory.pointXY(-88.331482, 32.325124); // 115.27399319083287
        Point nearestPoint = shapeFactory.pointXY(139.76185,35.6866); // 115.2748159708515
        double radius = 10d;

        Circle circle = shapeFactory.circle(point, 10);
        double distVincenty = DistanceUtils.radians2Dist(DistanceUtils.distVincentyRAD(
                toRadians(point.getX()), toRadians(point.getY()),
                toRadians(nearestPoint.getX()), toRadians(nearestPoint.getY())), 6356752.314245);
        System.out.println(distVincenty);
        System.out.println(sc.calcDistance(nearestPoint, circle.getCenter()));
        System.out.println(DistanceUtils.distHaversineRAD(point.getX(), point.getY(), nearestPoint.getX(), nearestPoint.getY()));
        double dist = DistanceUtils.degrees2Dist(
                DistanceUtils.distLawOfCosinesRAD(point.getX(), point.getY(), nearestPoint.getX(), nearestPoint.getY()),
                6356752.314245);
        System.out.println(dist);

    }
}
