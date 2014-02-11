package org.geoserver.extension;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.geoserver.extension.CLCProcess.StatisticContainer;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.JTS;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.google.common.util.concurrent.AtomicDouble;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class UrbanGridProcess implements GSProcess {

	public static final String IMP_2006_SHP = "imp_2006.shp";
	
	public static final String IMP_2009_SHP = "imp_2009.shp";

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

	public static final String PROJ_4326 = PROJ_HEADER + GEOGCS_4326
			+ PROJ_FOOTER;
	
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
		if (index == SEVENTH_INDEX
				&& (subId == null || subId.isEmpty() || !(subId
						.equalsIgnoreCase("a") || subId.equalsIgnoreCase("b") || subId
							.equalsIgnoreCase("c")))) {
			throw new IllegalArgumentException("Wrong subindex for index 7");
		}

		if(referenceCoverage == null && nowCoverage == null){
			throw new IllegalArgumentException("No input Coverage provided");
		}
		
		// If the index is not 8-9-10 then the input Urban Grids must be loaded
		// from the shp file.
		if (index < EIGHTH_INDEX) {

			// If index is 7a raster calculation can be executed
			if (index == SEVENTH_INDEX && subId.equalsIgnoreCase("a")) {
				Set<Integer> classes = new TreeSet<Integer>();
				classes.add(Integer.valueOf(1));
				return new CLCProcess().execute(referenceCoverage, nowCoverage,
						classes, CLCProcess.FIRST_INDEX, pixelArea, rois, null,
						null);
			}

			int numThreads = rois.size();
			
			boolean area = false;
			
			switch(index){
			case FIFTH_INDEX:
				area = true;
				break;
			case SIXTH_INDEX:
				area = false;
				break;
			case SEVENTH_INDEX:
				area = true;
			}
			
			if(referenceCoverage != null){
				latch = new CountDownLatch(numThreads); 
				//
				File file = new File(IMP_2006_SHP);
				Map map = new HashMap();
				map.put("url", file.toURL());
				DataStore dataStore = DataStoreFinder.getDataStore(map);
				
				ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads,
						numThreads, 60, TimeUnit.SECONDS,
						new ArrayBlockingQueue<Runnable>(1000000));

				for (Geometry geo : rois) {
					List<Double> areas = new ArrayList<Double>();
					MyRunnable run = new MyRunnable(geo, dataStore,areas,area);
					executor.execute(run);
				}
			}
			
			if(nowCoverage != null){
				
			}
			


		} else if (index == EIGHTH_INDEX) {
			// Raster elaboration

		} else {
			// For the index 9-10 Zonal Stats are calculated

			Set<Integer> classes = new TreeSet<Integer>();
			classes.add(Integer.valueOf(1));

			if (index == TENTH_INDEX && coeff != null) {
				return new CLCProcess().execute(referenceCoverage, nowCoverage,
						classes, CLCProcess.THIRD_INDEX, pixelArea, rois,
						populations, coeff);
			} else if (index == NINTH_INDEX) {
				return new CLCProcess().execute(referenceCoverage, nowCoverage,
						classes, CLCProcess.THIRD_INDEX, pixelArea, rois,
						populations, Double.valueOf(1));
			} else {
				throw new IllegalArgumentException(
						"No coefficient provided for the selected index");
			}
		}

		return null;

	}

	class MyRunnable implements Runnable {

		private Geometry geo;
		private DataStore ds;
		private List<Double> values;
		private final boolean area;

		public MyRunnable(Geometry geo, DataStore ds, List<Double> values,
				boolean area) {
			this.geo = geo;
			this.ds = ds;
			this.values=values;
			this.area=area;
		}

		@Override
		public void run() {

			String typeName = null;
			FeatureSource source = null;
			try {
				typeName = ds.getTypeNames()[0];
				source = ds.getFeatureSource(typeName);
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (source == null) {
				return;// FIXME SHOULD THROW AN EXCEPTION?
			}

			CoordinateReferenceSystem sourceCRS = source.getInfo().getCRS();

			FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

			Filter filter = ff.within(ff.property("THE_GEOM"), ff.literal(geo));

			FeatureCollection coll = null;
			try {
				coll = source.getFeatures(filter);
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (coll == null) {
				return;// FIXME SHOULD THROW AN EXCEPTION?
			}

			FeatureIterator iter = coll.features();

			double perimeter = 0;
			
			try {
				while (iter.hasNext()) {
					SimpleFeature feature = (SimpleFeature) iter.next();
					Geometry sourceGeometry = (Geometry) feature
							.getDefaultGeometry();

					// Reproject to the Lambert Equal Area
					Point center = sourceGeometry.getCentroid();

					String wkt = PROJ_4326.replace("%LAT0%",
							String.valueOf(center.getY()));
					wkt = wkt.replace("%LON0%", String.valueOf(center.getX()));

					final CoordinateReferenceSystem targetCRS = CRS
							.parseWKT(wkt);
					
					MathTransform trans = CRS.findMathTransform(sourceCRS, targetCRS);
					
					Geometry geoPrj = JTS.transform(sourceGeometry, trans);
					
					if(area){
						values.add(geoPrj.getArea());
					}else{
						perimeter += geoPrj.getLength();
					}
				}

				if(!area){
					values.add(perimeter);
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			latch.countDown();
		}
	}
}
