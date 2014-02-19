package org.geoserver.wps.gs.soilsealing;

import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.media.jai.TiledImage;

import org.geoserver.wps.gs.soilsealing.CLCProcess.StatisticContainer;
import org.geoserver.wps.gs.soilsealing.UrbanGridProcess;
import org.geoserver.wps.gs.soilsealing.UrbanGridProcess.ListContainer;
import org.geoserver.wps.gs.soilsealing.UrbanGridProcess.MyRunnable;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.GeoTools;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.referencing.CRS;
import org.geotools.test.TestData;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class UrbanGridProcessTest {

    public static final int DEF_H = 256;

    public static final int DEF_W = 256;

    public static final int DEF_TILE_H = 32;

    public static final int DEF_TILE_W = 32;

    public static final double DELTA = 0.0001;
    
    private static final String ECKERTIVWKT = "PROJCS[\"World_Eckert_IV\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Eckert_IV\"],PARAMETER[\"Central_Meridian\",0.0],UNIT[\"Meter\",1.0]]";

    private static CoordinateReferenceSystem crs;

    private static GridCoverage2D referenceCoverage;

    private static GridCoverage2D nowCoverage;

    private static ArrayList<Geometry> geomListRaster;

    private static ArrayList<Geometry> geomListUtm32N;

    private static UrbanGridProcess urbanProcess;

    private static String pathToRefShp = "src/test/resources/org/geoserver/wps/gs/soilsealing/test-data/referenceCov.shp";

    private static String pathToCurShp = "src/test/resources/org/geoserver/wps/gs/soilsealing/test-data/nowCov.shp";

    private static String refShp = "referenceCov.shp";

    private static String curShp = "nowCov.shp";

    private static double totalPerimeterRef;

    private static List<Double> areasRef;

    private static double totalAreaRef;

    private static double totalPerimeterCur;

    private static List<Double> areasCur;

    private static double totalAreaCur;

    @BeforeClass
    public static void initialSetup() throws NoSuchAuthorityCodeException, FactoryException,
            IOException {
        crs = CRS.decode("EPSG:32632");

        referenceCoverage = createImage(true);
        nowCoverage = createImage(false);

        // Geometries in Raster space for indexes 7a-8-9-10
        geomListRaster = new ArrayList<Geometry>();

        GeometryFactory fact = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[5];
        for (int i = 0; i < coordinates.length; i++) {
            if (i == 0 || i == 4) {
                coordinates[i] = new Coordinate(0, 0);
            } else if (i == 3) {
                coordinates[i] = new Coordinate(DEF_W / 2, 0);
            } else if (i == 1) {
                coordinates[i] = new Coordinate(0, DEF_H / 2);
            } else {
                coordinates[i] = new Coordinate(DEF_W / 2, DEF_H / 2);
            }
        }
        LinearRing linear = new GeometryFactory().createLinearRing(coordinates);
        Polygon poly = new Polygon(linear, null, fact);

        geomListRaster.add(poly);

        // Geometries in UTM 32 N
        geomListUtm32N = new ArrayList<Geometry>();
        
        
        Coordinate[] coordinates2 = new Coordinate[5];
        for (int i = 0; i < coordinates2.length; i++) {
            if (i == 0 || i == 4) {
                coordinates2[i] = new Coordinate(0, DEF_H);
            } else if (i == 3) {
                coordinates2[i] = new Coordinate(0, DEF_H / 2);
            } else if (i == 1) {
                coordinates2[i] = new Coordinate(DEF_W / 2, DEF_H);
            } else {
                coordinates2[i] = new Coordinate(DEF_W / 2, DEF_H / 2);
            }
        }
        LinearRing linear2 = new GeometryFactory().createLinearRing(coordinates2);
        Polygon poly2 = new Polygon(linear2, null, fact);

        poly2.setSRID(32632);
        geomListUtm32N.add(poly2);

        urbanProcess = new UrbanGridProcess(pathToRefShp, pathToCurShp);

        //HAConverter
        double converter = UrbanGridProcess.HACONVERTER;
        
        // Reference
        totalAreaRef = 125*converter;
        areasRef = new ArrayList<Double>(2);
        areasRef.add(25d*converter);
        areasRef.add(100d*converter);
        calculatePerimeters(poly2, true);
        // Current
        totalAreaCur = 115*converter;
        areasCur = new ArrayList<Double>(2);
        areasCur.add(25d*converter);
        areasCur.add(90d*converter);
        calculatePerimeters(poly2, false);
    }

    @Test
    public void testIndex5() throws Exception {
        List<StatisticContainer> results = urbanProcess.execute(referenceCoverage, null, 5, null,
                null, geomListUtm32N, null, null);

        // Expected results
        double sut = totalAreaRef;
        double sud = totalAreaRef - areasRef.get(areasRef.size() - 1);
        double expected = sud / sut;

        double calculated = results.get(0).getResults()[0];

        Assert.assertEquals(expected, calculated, DELTA);
    }

    @Test
    public void testIndex5img2() throws Exception {
        List<StatisticContainer> results = urbanProcess.execute(referenceCoverage, nowCoverage, 5, null,
                null, geomListUtm32N, null, null);

        // Expected results Reference
        double sutRef = totalAreaRef;
        double sudRef = totalAreaRef - areasRef.get(areasRef.size() - 1);
        double expectedRef = sudRef / sutRef;

        double calculatedRef = results.get(0).getResultsRef()[0];

        Assert.assertEquals(expectedRef, calculatedRef, DELTA);
        
        // Expected results Current
        double sutCur = totalAreaCur;
        double sudCur = totalAreaCur - areasCur.get(areasCur.size() - 1);
        double expectedCur = sudCur / sutCur;

        double calculatedCur = results.get(0).getResultsNow()[0];

        Assert.assertEquals(expectedCur, calculatedCur, DELTA);
    }

    @Test
    public void testIndex6() throws Exception {
        List<StatisticContainer> results = urbanProcess.execute(referenceCoverage, null, 6, null,
                null, geomListUtm32N, null, null);
        
        
        

    }

    @Test
    public void testIndex6img2() throws Exception {

    }

    @Test
    public void testIndex7() throws Exception {

    }

    @Test
    public void testIndex7img2() throws Exception {

    }

    @Test
    public void testIndex8() throws Exception {

    }

    @Test
    public void testIndex8img2() throws Exception {

    }

    @Test
    public void testIndex9() throws Exception {

    }

    @Test
    public void testIndex10() throws Exception {

    }

    public static GridCoverage2D createImage(boolean reference) {

        SampleModel sm = new ComponentSampleModel(DataBuffer.TYPE_BYTE, DEF_W, DEF_H, 1, DEF_W,
                new int[] { 0 });

        TiledImage img = new TiledImage(sm, DEF_TILE_W, DEF_TILE_H);

        int minX = 0;
        int maxX = 0;
        int minY = 0;
        int maxY = 0;

        if (reference) {
            minX = 10;
            maxX = 20;
            minY = 10;
            maxY = 20;
        } else {
            minX = 11;
            maxX = 20;
            minY = 10;
            maxY = 20;
        }

        int minPolStart = 0;
        int maxPolStart = 5;

        for (int i = minPolStart; i < maxPolStart; i++) {
            for (int j = minPolStart; j < maxPolStart; j++) {
                img.setSample(i, j, 0, 1);
            }
        }

        for (int i = minX; i < maxX; i++) {
            for (int j = minY; j < maxY; j++) {
                img.setSample(i, j, 0, 1);
            }
        }

        Envelope envelope = new Envelope2D(crs, 0, 0, DEF_W, DEF_H);

        GridCoverage2D result = new GridCoverageFactory(GeoTools.getDefaultHints()).create("test",
                img, envelope);

        return result;
    }

    private static void calculatePerimeters(Geometry geom, boolean reference)
            throws IOException {

        String path = null;

        if (reference) {
            path = refShp;
        } else {
            path = curShp;
        }

        // ShapeFile selection
        File file = TestData.file(UrbanGridProcessTest.class, path);

        Map map = new HashMap();
        map.put("url", file.toURL());
        // Datastore creation
        DataStore ds = DataStoreFinder.getDataStore(map);

        // Selection of the Feature Source associated to the datastore
        String typeName = null;
        FeatureSource source = null;
        try {
            typeName = ds.getTypeNames()[0];
            source = ds.getFeatureSource(typeName);
        } catch (IOException e) {
            throw new ProcessException(e);
        }
        // If the Feature source is not present, an exception is thrown
        if (source == null) {
            throw new ProcessException("Source datastore not found");
        }

        FeatureType schema = source.getSchema();

        // usually "THE_GEOM" for shapefiles
        String geometryPropertyName = schema.getGeometryDescriptor().getLocalName();
        if (geometryPropertyName == null || geometryPropertyName.isEmpty()) {
            geometryPropertyName = "THE_GEOM";
        }

        // ShapeFile CRS
        CoordinateReferenceSystem sourceCRS = schema.getGeometryDescriptor()
                .getCoordinateReferenceSystem();
        // Filter on the data store by selecting only the geometries contained into the input Geometry
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

        Filter filter = ff.within(ff.property(geometryPropertyName), ff.literal(geom));
        // Feature collection selection
        FeatureCollection coll = null;
        try {
            //coll = source.getFeatures();
            coll = source.getFeatures(filter);
        } catch (IOException e) {
            throw new ProcessException(e);
        }

        if (coll == null) {
            throw new ProcessException("Source Feature collection not found");
        }

        // Iterator on the features
        FeatureIterator iter = coll.features();

        double totalPerimeter = 0;

        // Selection of the inner list
        //List<Double> areas = new ArrayList<Double>();

        //double totalArea = 0;
        // Cycle on each polygon
        try {
            while (iter.hasNext()) {
                SimpleFeature feature = (SimpleFeature) iter.next();
                Geometry sourceGeometry = (Geometry) feature.getDefaultGeometry();                
                // If the geometry is a Polygon, then the operations are executed
                // reprojection of the polygon
                Geometry geoPrj = reprojectToEqualArea(sourceCRS, sourceGeometry);
                if(geoPrj!=null){
                 // Area/Perimeter calculation
                    //double area = geoPrj.getArea();
                    //areas.add(area);
                    //totalArea += area;
                    totalPerimeter += geoPrj.getLength();
                }
            }
        } catch (Exception e) {
            throw new ProcessException(e);
        } finally {
            // Iterator closure
            iter.close();
        }
        // Datastore dispose
        ds.dispose();

        if (reference) {
            //areasRef = areas;
            //totalAreaRef = totalArea;
            totalPerimeterRef = totalPerimeter;
        } else {
            //areasCur = areas;
            //totalAreaCur = totalArea;
            totalPerimeterCur = totalPerimeter;
        }
    }

    private static Geometry reprojectToEqualArea(CoordinateReferenceSystem sourceCRS,
            Geometry sourceGeometry) throws FactoryException, TransformException {
        // Reproject to the Lambert Equal Area
        
        final ReferencedEnvelope env = new ReferencedEnvelope(sourceGeometry.getEnvelopeInternal(), sourceCRS);
        // Geometry center used for centering the reprojection on the Geometry(reduces distance artifacts)
        double lat = env.getMedian(1);
        double lon = env.getMedian(0);
        // Geometry center used for centering the reprojection on the Geometry(reduces distance artifacts)
        Point center = sourceGeometry.getCentroid();
        // Creation of the MathTransform associated to the reprojection
        MathTransform transPoint = CRS.findMathTransform(sourceCRS, CRS.decode("EPSG:4326"),true);
        Point centerRP = (Point) JTS.transform(center, transPoint);
        lon = centerRP.getY();
        lat = centerRP.getX();
        // Creation of a wkt for the selected Geometry
        String wkt = UrbanGridProcess.PROJ_4326.replace("%LAT0%", String.valueOf(lat));
        wkt = wkt.replace("%LON0%", String.valueOf(lon));
        // Parsing of the selected WKT
        final CoordinateReferenceSystem targetCRS = CRS.parseWKT(wkt);
        // Creation of the MathTransform associated to the reprojection
        MathTransform trans = CRS.findMathTransform(sourceCRS, targetCRS,true);
        // Geometry reprojection
        Geometry geoPrj;
        if (!trans.isIdentity()) {
            geoPrj = JTS.transform(sourceGeometry, trans);
        } else {
            geoPrj = sourceGeometry;
        }
        return geoPrj;
    }

}
