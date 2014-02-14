package org.geoserver.extension;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geoserver.extension.CLCProcess.StatisticContainer;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.JTS;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class UrbanGridProcess implements GSProcess {

    public static final Logger LOGGER = Logger.getLogger(UrbanGridProcess.class.toString());

    public static final String IMP_2006_SHP = "imp_2006.shp";

    public static final String IMP_2009_SHP = "imp_2009.shp";

    public static final double HACONVERTER = 0.0001;

    public static final int FIFTH_INDEX = 5;

    public static final int SIXTH_INDEX = 6;

    public static final int SEVENTH_INDEX = 7;

    public static final int EIGHTH_INDEX = 8;

    public static final int NINTH_INDEX = 9;

    public static final int TENTH_INDEX = 10;

    public static final String PROJ_HEADER = "PROJCS[\"Local area projection/ LOCAL_AREA_PROJECTION\",";

    public static final String PROJ_FOOTER = ",PROJECTION[\"Lambert Azimuthal Equal Area\", AUTHORITY[\"EPSG\",\"9820\"]],"
            + "PARAMETER[\"latitude_of_center\", %LAT0%], PARAMETER[\"longitude_of_center\", %LON0%],"
            + "PARAMETER[\"false_easting\", 0.0], PARAMETER[\"false_northing\", 0.0],"
            + "UNIT[\"m\", 1.0], AXIS[\"Northing\", NORTH], AXIS[\"Easting\", EAST], AUTHORITY[\"EPSG\",\"3035\"]]";

    public static final String GEOGCS_4326 = "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,"
            + "AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],"
            + "UNIT[\"degree\",0.01745329251994328,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4326\"]]";

    public static final String PROJ_4326 = PROJ_HEADER + GEOGCS_4326 + PROJ_FOOTER;

    public static final CoordinateReferenceSystem UTM32N;

    static {
        CoordinateReferenceSystem crs = null;
        try {
            crs = CRS.decode("EPSG:32632");
        } catch (Exception e) {
            e.printStackTrace();
        }

        UTM32N = crs;

    }

    private CountDownLatch latch;

    // HP to verify
    // HP1 = admin geometries in Raster space, for index 7a-8-9-10; in UTM Zone
    // 32 for other indexes
    // HP2 = Coverages already cropped and transformed to the Raster Space

    @DescribeResult(name = "UrbanGridProcess", description = "Urban Grid indexes", type = List.class)
    public List<StatisticContainer> execute(
            @DescribeParameter(name = "reference", description = "Name of the reference raster") GridCoverage2D referenceCoverage,
            @DescribeParameter(name = "now", description = "Name of the new raster") GridCoverage2D nowCoverage,
            @DescribeParameter(name = "index", min = 1, description = "Index to calculate") int index,
            @DescribeParameter(name = "subindex", min = 0, description = "String indicating which sub-index must be calculated") String subId,
            @DescribeParameter(name = "pixelarea", min = 0, description = "Pixel Area") Double pixelArea,
            @DescribeParameter(name = "rois", min = 1, description = "Administrative Areas") List<Geometry> rois,
            @DescribeParameter(name = "populations", min = 0, description = "Populations for each Area") List<List<Integer>> populations,
            @DescribeParameter(name = "coefficient", min = 0, description = "Administrative Areas") Double coeff)
            throws Exception {

        // Check on the index 7
        boolean nullSubId = subId == null || subId.isEmpty();
        boolean subIndexA = !nullSubId && subId.equalsIgnoreCase("a");
        boolean subIndexC = !nullSubId && subId.equalsIgnoreCase("c");
        boolean subIndexB = !nullSubId && subId.equalsIgnoreCase("b");
        if (index == SEVENTH_INDEX && (nullSubId || !(subIndexA || subIndexB || subIndexC))) {
            throw new IllegalArgumentException("Wrong subindex for index 7");
        }

        if (referenceCoverage == null && nowCoverage == null) {
            throw new IllegalArgumentException("No input Coverage provided");
        }

        int numThreads = rois.size();

        boolean area = false;

        Set<Integer> classes = new TreeSet<Integer>();
        classes.add(Integer.valueOf(1));

        switch (index) {
        case FIFTH_INDEX:
            area = true;
            break;
        case SIXTH_INDEX:
            area = false;
            break;
        case SEVENTH_INDEX:
            area = true;
            // If index is 7a raster calculation can be executed
            if (subIndexA) {
                return new CLCProcess().execute(referenceCoverage, nowCoverage, classes,
                        CLCProcess.FIRST_INDEX, pixelArea, rois, null, null, true);
            }
        case EIGHTH_INDEX:
            // Raster elaboration
            break;
        // For the indexes 9-10 Zonal Stats are calculated
        case NINTH_INDEX:
            return new CLCProcess().execute(referenceCoverage, nowCoverage, classes,
                    CLCProcess.THIRD_INDEX, pixelArea, rois, populations, Double.valueOf(1), null);
        case TENTH_INDEX:
            if (coeff != null) {
                return new CLCProcess().execute(referenceCoverage, nowCoverage, classes,
                        CLCProcess.THIRD_INDEX, pixelArea, rois, populations, coeff, null);
            } else {
                throw new IllegalArgumentException("No coefficient provided for the selected index");
            }
        default:
            throw new IllegalArgumentException("Wrong index declared");
        }

        // If index is not 7a-8-9-10 then the input Urban Grids must be loaded
        // from the shp file.
        
        double[] statsRef = null;
        double[] statsNow = null;

        if (referenceCoverage != null) {
            statsRef = prepareResults(IMP_2006_SHP, index, rois, subIndexB, numThreads, area);
        }

        if (nowCoverage != null) {
            statsNow = prepareResults(IMP_2009_SHP, index, rois, subIndexB, numThreads, area);
        }

        List<StatisticContainer> results = accumulateResults(rois, statsRef, statsNow);

        return results;

    }

    private List<StatisticContainer> accumulateResults(List<Geometry> rois, double[] reference,
            double[] now) {

        int numGeo = rois.size();

        List<StatisticContainer> results = new ArrayList<StatisticContainer>(numGeo);

        if (reference == null && now == null) {
            throw new ProcessException("No result has been calculated");
        } else if (reference != null && now == null) {
            // check on the dimensions
            if (numGeo != reference.length) {
                throw new ProcessException(
                        "Geometries and their results don't have the same dimensions");
            }
            for (int i = 0; i < numGeo; i++) {
                Geometry geo = rois.get(i);

                StatisticContainer container = new StatisticContainer(geo,
                        new double[] { reference[i] }, null);
                results.add(container);
            }
        } else if (reference == null && now != null) {
            // check on the dimensions
            if (numGeo != now.length) {
                throw new ProcessException(
                        "Geometries and their results don't have the same dimensions");
            }
            for (int i = 0; i < numGeo; i++) {
                Geometry geo = rois.get(i);

                StatisticContainer container = new StatisticContainer(geo, new double[] { now[i] },
                        null);
                results.add(container);
            }
        } else {
            // check on the dimensions
            if (numGeo != now.length || numGeo != reference.length) {
                throw new ProcessException(
                        "Geometries and their results don't have the same dimensions");
            }
            for (int i = 0; i < numGeo; i++) {
                Geometry geo = rois.get(i);
                double percentual = ((now[i] - reference[i]) / reference[i]) * 100;
                StatisticContainer container = new StatisticContainer(geo,
                        new double[] { percentual }, null);
                results.add(container);
            }
        }
        return results;
    }

    private double[] prepareResults(String inputShp, int index, List<Geometry> rois,
            boolean subIndexB, int numThreads, boolean area) throws MalformedURLException,
            IOException, InterruptedException, FactoryException, TransformException {

        List<ListContainer> referenceGeom = caculateGeometries(inputShp, rois, numThreads, area);

        double[] stats = new double[rois.size()];

        int counter = 0;

        for (ListContainer container : referenceGeom) {
            List<Double> areas = container.getList();

            int numPolyNotMax = areas.size() - 1;
            double polyMaxArea = areas.get(numPolyNotMax);

            double sut = container.getTotalArea();
            double sud = sut - polyMaxArea;

            switch (index) {
            case FIFTH_INDEX:
                stats[counter] = sud / sut;
                break;
            case SIXTH_INDEX:
                Geometry geo = rois.get(counter);
                Geometry geoPrj = reprojectToEqualArea(UTM32N, geo);

                double areaAdmin = geoPrj.getArea();

                stats[counter] = (container.getTotalPerimeter() / areaAdmin) / HACONVERTER;
                break;
            case SEVENTH_INDEX:
                if (subIndexB) {
                    stats[counter] = (polyMaxArea / sut) * 100;
                } else {
                    stats[counter] = (sud / numPolyNotMax) * HACONVERTER;
                }
            }
            // Counter update
            counter++;
        }

        return stats;
    }

    private List<ListContainer> caculateGeometries(String inputShp, List<Geometry> rois,
            int numThreads, boolean area) throws MalformedURLException, IOException,
            InterruptedException {
        latch = new CountDownLatch(numThreads);
        //
        File file = new File(inputShp);
        Map map = new HashMap();
        map.put("url", file.toURL());
        DataStore dataStore = DataStoreFinder.getDataStore(map);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads, numThreads, 60,
                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000000));

        List<ListContainer> allLists = new ArrayList<ListContainer>(numThreads);

        for (Geometry geo : rois) {
            ListContainer container = new ListContainer();
            allLists.add(container);
            MyRunnable run = new MyRunnable(geo, dataStore, container, area);
            executor.execute(run);
        }

        latch.await();

        executor.shutdown();

        executor.awaitTermination(30, TimeUnit.SECONDS);

        return allLists;
    }

    private Geometry reprojectToEqualArea(CoordinateReferenceSystem sourceCRS,
            Geometry sourceGeometry) throws FactoryException, TransformException {
        // Reproject to the Lambert Equal Area
        Point center = sourceGeometry.getCentroid();

        String wkt = PROJ_4326.replace("%LAT0%", String.valueOf(center.getY()));
        wkt = wkt.replace("%LON0%", String.valueOf(center.getX()));

        final CoordinateReferenceSystem targetCRS = CRS.parseWKT(wkt);

        MathTransform trans = CRS.findMathTransform(sourceCRS, targetCRS);

        Geometry geoPrj = JTS.transform(sourceGeometry, trans);
        return geoPrj;
    }

    class MyRunnable implements Runnable {

        private Geometry geo;

        private DataStore ds;

        private ListContainer values;

        private final boolean area;

        public MyRunnable(Geometry geo, DataStore ds, ListContainer values, boolean area) {
            this.geo = geo;
            this.ds = ds;
            this.values = values;
            this.area = area;
        }

        @Override
        public void run() {

            String typeName = null;
            FeatureSource source = null;
            try {
                typeName = ds.getTypeNames()[0];
                source = ds.getFeatureSource(typeName);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getMessage());
                throw new ProcessException(e);
            }

            if (source == null) {
                throw new ProcessException("Source datastore not found");
            }

            CoordinateReferenceSystem sourceCRS = source.getInfo().getCRS();

            FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

            Filter filter = ff.within(ff.property("THE_GEOM"), ff.literal(geo));

            FeatureCollection coll = null;
            try {
                coll = source.getFeatures(filter);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getMessage());
                throw new ProcessException(e);
            }

            if (coll == null) {
                throw new ProcessException("Source Feature collection not found");
            }

            FeatureIterator iter = coll.features();

            double totalPerimeter = 0;

            // Selection of the inner list
            List<Double> areas = new ArrayList<Double>();

            double totalArea = 0;

            try {
                while (iter.hasNext()) {
                    SimpleFeature feature = (SimpleFeature) iter.next();
                    Geometry sourceGeometry = (Geometry) feature.getDefaultGeometry();

                    Geometry geoPrj = reprojectToEqualArea(sourceCRS, sourceGeometry);

                    if (area) {
                        double area = geoPrj.getArea();
                        areas.add(area);
                        totalArea += area;
                    } else {
                        totalPerimeter += geoPrj.getLength();
                    }
                }

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage());
                throw new ProcessException(e);
            }

            if (area) {
                values.setList(areas);
                values.setTotalArea(totalArea);
            } else {
                values.setTotalPerimeter(totalPerimeter);
            }

            latch.countDown();
        }
    }

    class ListContainer {

        private List<Double> list;

        private Double totalArea;

        private Double totalPerimeter;

        ListContainer() {
        }

        public List<Double> getList() {
            return list;
        }

        public void setList(List<Double> list) {
            Collections.sort(list);
            this.list = list;
        }

        public double getTotalArea() {
            return totalArea;
        }

        public void setTotalArea(double totalArea) {
            this.totalArea = totalArea;
        }

        public Double getTotalPerimeter() {
            return totalPerimeter;
        }

        public void setTotalPerimeter(Double totalPerimeter) {
            this.totalPerimeter = totalPerimeter;
        }

        public void setTotalArea(Double totalArea) {
            this.totalArea = totalArea;
        }

    }

}
