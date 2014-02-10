package org.geoserver.extension;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
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
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.google.common.util.concurrent.AtomicDouble;
import com.vividsolutions.jts.geom.Geometry;

public class UrbanGridProcess implements GSProcess {

	public static final int FIFTH_INDEX = 5;

	public static final int SIXTH_INDEX = 6;

	public static final int SEVENTH_INDEX = 7;

	public static final int EIGHTH_INDEX = 8;

	public static final int NINTH_INDEX = 9;

	public static final int TENTH_INDEX = 10;

	// HP to verify
	// HP1 = admin geometries in Raster space, for index 7a-8-9-10; in UTM Zone 32 for other indexes 
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
				&& (subId == null || subId.isEmpty() || 
				!(subId.equalsIgnoreCase("a") 
					|| subId.equalsIgnoreCase("b") 
					|| subId.equalsIgnoreCase("c")))) {
			throw new IllegalArgumentException("Wrong subindex for index 7");
		}

		// If the index is not 8-9-10 then the input Urban Grids must be loaded
		// from the shp file.
		if (index < EIGHTH_INDEX) {
			
			// If index is 7a raster calculation can be executed
			if(index == SEVENTH_INDEX && subId.equalsIgnoreCase("a")){
				Set<Integer> classes = new TreeSet<Integer>();
				classes.add(Integer.valueOf(1));
				return new CLCProcess().execute(referenceCoverage, nowCoverage,
						classes, CLCProcess.FIRST_INDEX, pixelArea, rois,
						null, null);
			}
			
			//
			File file = new File("imp_2006.shp");
			Map map = new HashMap();
			map.put("url", file.toURL());
			DataStore dataStore = DataStoreFinder.getDataStore(map);
			String typeName = dataStore.getTypeNames()[0];
			FeatureSource source = dataStore.getFeatureSource(typeName);

			CoordinateReferenceSystem sourceCrs = source.getInfo().getCRS();

			FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

			Filter filter;

			int numThreads = rois.size();

			ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads,
					numThreads, 60, TimeUnit.SECONDS,
					new ArrayBlockingQueue<Runnable>(1000000));

			for (Geometry geo : rois) {

				filter = ff.within(ff.property("THE_GEOM"), ff.literal(geo));

				FeatureCollection coll = source.getFeatures(filter);
			}

		} else if (index == EIGHTH_INDEX) {
			// Raster elaboration

		} else {
			// Zonal Stats

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
	
	
	
	static class MyRunnable implements Runnable{


		
		private Geometry geo;
		private DataStore ds;
		private AtomicDouble counter;
		private double[] areas;
		private boolean update=false;
		private Boolean perimeter;




		public MyRunnable(Geometry geo, DataStore ds, AtomicDouble counter, Boolean perimeter){
			this.geo=geo;
			this.ds=ds;
			this.counter=counter;
			this.perimeter=perimeter;
			
			if(counter!=null){
				this.update=true;
			}
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
			
			if(source==null){
				return;//FIXME SHOULD THROW AN EXCEPTION?
			}
			
			CoordinateReferenceSystem sourceCrs = source.getInfo().getCRS();

			FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

			Filter filter = ff.within(ff.property("THE_GEOM"), ff.literal(geo));
			
			FeatureCollection coll = null;
			try {
				coll = source.getFeatures(filter);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			if(coll==null){
				return;//FIXME SHOULD THROW AN EXCEPTION?
			}
			
			Set<Double> values;
			
			if(update){
				values = null;
			}else{
				values = new TreeSet<Double>();
			}
			
			FeatureIterator iter = coll.features();
			
			while(iter.hasNext()){
				SimpleFeature feature = (SimpleFeature) iter.next();
			    Geometry sourceGeometry = (Geometry) feature.getDefaultGeometry();
				
			    //Reproject to 
			    
			    
			    
				
				
			}
			
			
		}
	}
}
