/* Copyright (c) 2001 - 2014 OpenPlans - www.openplans.org. All rights 
 * reserved. This code is licensed under the GPL 2.0 license, available at the 
 * root application directory.
 */
package org.geoserver.wps.gs.soilsealing;

import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.logging.Logger;

import javax.media.jai.RenderedOp;

import net.sf.json.JSONSerializer;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.data.util.CoverageUtils;
import org.geoserver.wps.WPSException;
import org.geoserver.wps.gs.ImportProcess;
import org.geoserver.wps.gs.ToFeature;
import org.geoserver.wps.gs.WFSLog;
import org.geoserver.wps.gs.soilsealing.CLCProcess.StatisticContainer;
import org.geoserver.wps.gs.soilsealing.SoilSealingAdministrativeUnit.AuSelectionType;
import org.geoserver.wps.gs.soilsealing.model.SoilSealingIndex;
import org.geoserver.wps.gs.soilsealing.model.SoilSealingOutput;
import org.geoserver.wps.gs.soilsealing.model.SoilSealingTime;
import org.geoserver.wps.ppio.FeatureAttribute;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.DataUtilities;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.IsEqualsToImpl;
import org.geotools.gce.imagemosaic.ImageMosaicFormat;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.resources.image.ImageUtilities;
import org.geotools.util.NullProgressListener;
import org.geotools.util.logging.Logging;
import org.opengis.coverage.grid.GridCoverageReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;

/**
 * Middleware process collecting the inputs for {@link CLCProcess} indexes.
 * 
 * @author geosolutions
 * 
 */
@DescribeProcess(title = "SoilSealingCLC", description = " Middleware process collecting the inputs for CLCProcess indexes")
public class SoilSealingCLCProcess extends SoilSealingMiddlewareProcess {

    private final static Logger LOGGER = Logging.getLogger(SoilSealingCLCProcess.class);
            
    /**
     * Default Constructor
     * 
     * @param catalog
     * @param geoserver
     */
    public SoilSealingCLCProcess(Catalog catalog, GeoServer geoserver) {
        super(catalog, geoserver);
    }

    /**
     * 
     * @param referenceName
     * @param defaultStyle
     * @param storeName
     * @param typeName
     * @param referenceFilter
     * @param nowFilter
     * @param classes
     * @param geocoderLayer
     * @param geocoderPopulationLayer
     * @param admUnits
     * @param admUnitSelectionType
     * @return
     * @throws IOException
     */
    @DescribeResult(name = "soilSealingCLC", description = "SoilSealing CLC Middleware Process", type = SoilSealingDTO.class)
    public SoilSealingDTO execute(
            @DescribeParameter(name = "name", description = "Name of the raster, optionally fully qualified (workspace:name)") String referenceName,
            @DescribeParameter(name = "defaultStyle", description = "Name of the raster default style") String defaultStyle,
            @DescribeParameter(name = "storeName", description = "Name of the destination data store to log info") String storeName,
            @DescribeParameter(name = "typeName", description = "Name of the destination feature type to log info") String typeName,
            @DescribeParameter(name = "referenceFilter", description = "Filter to use on the raster data", min = 1) Filter referenceFilter,
            @DescribeParameter(name = "nowFilter", description = "Filter to use on the raster data", min = 0) Filter nowFilter,
            @DescribeParameter(name = "index", min = 1, description = "Index to calculate") int index,
            @DescribeParameter(name = "subindex", min = 0, description = "String indicating which sub-index must be calculated {a,b,c}") String subIndex,
            @DescribeParameter(name = "classes", collectionType = Integer.class, min = 0, description = "The domain of the classes used in input rasters") Set<Integer> classes,
            @DescribeParameter(name = "geocoderLayer", description = "Name of the geocoder layer, optionally fully qualified (workspace:name)") String geocoderLayer,
            @DescribeParameter(name = "geocoderPopulationLayer", description = "Name of the geocoder population layer, optionally fully qualified (workspace:name)") String geocoderPopulationLayer,
            @DescribeParameter(name = "admUnits", min = 1, description = "Comma Separated list of Administrative Units") String admUnits,
            @DescribeParameter(name = "admUnitSelectionType", min = 1, description = "Administrative Units Slection Type") AuSelectionType admUnitSelectionType)
            throws IOException {

        // ///////////////////////////////////////////////
        // Sanity checks ...
        // get the original Coverages
        CoverageInfo ciReference = catalog.getCoverageByName(referenceName);
        if (ciReference == null) {
            throw new WPSException("Could not find coverage " + referenceName);
        }

        // get access to GeoCoding Layers
        FeatureTypeInfo geoCodingReference = catalog.getFeatureTypeByName(geocoderLayer);
        FeatureTypeInfo populationReference = catalog.getFeatureTypeByName(geocoderPopulationLayer);
        if (geoCodingReference == null || populationReference == null) {
            throw new WPSException("Could not find geocoding reference layers (" + geocoderLayer
                    + " / " + geocoderPopulationLayer + ")");
        }

        if (admUnits == null || admUnits.isEmpty()) {
            throw new WPSException("No Administrative Unit has been specified.");
        }
        // ///////////////////////////////////////////////
        
        // ///////////////////////////////////////////////
        // SoilSealing outcome variables ...
        RenderedOp result = null;
        GridCoverage2D nowCoverage = null;
        GridCoverage2D referenceCoverage = null;
        List<String> municipalities = new LinkedList<String>();
        List<Geometry> rois = new LinkedList<Geometry>();
        List<List<Integer>> populations = new LinkedList<List<Integer>>();
        populations.add(new LinkedList<Integer>());
        if (nowFilter != null) populations.add(new LinkedList<Integer>());
        // ///////////////////////////////////////////////

        // ///////////////////////////////////////////////
        // Logging to WFS variables ...
        final String wsName = ciReference.getNamespace().getPrefix();
        final UUID uuid = UUID.randomUUID();
        SimpleFeatureCollection features = null;
        Filter filter = null;
        ToFeature toFeatureProcess = new ToFeature();
        WFSLog wfsLogProcess = new WFSLog(geoserver);
        // ///////////////////////////////////////////////

        try {

            final String referenceYear = ((IsEqualsToImpl) referenceFilter).getExpression2().toString().substring(0, 4);
            final String currentYear = (nowFilter == null ? null : ((IsEqualsToImpl) nowFilter).getExpression2().toString().substring(0, 4));

            // //////////////////////////////////////
            // Scan the geocoding layers and prepare
            // the geometries and population values.
            // //////////////////////////////////////
            final CoordinateReferenceSystem referenceCrs = ciReference.getCRS();
            prepareAdminROIs(nowFilter, admUnits, admUnitSelectionType, ciReference,
                    geoCodingReference, populationReference, municipalities, rois, populations,
                    referenceYear, currentYear,
                    referenceCrs, true);
            
            // read reference coverage
            GridCoverageReader referenceReader = ciReference.getGridCoverageReader(null, null);
            ParameterValueGroup readParametersDescriptor = referenceReader.getFormat().getReadParameters();

            // get params for this coverage and override what's needed
            Map<String, Serializable> defaultParams = ciReference.getParameters();
            GeneralParameterValue[] params = CoverageUtils.getParameters(readParametersDescriptor, defaultParams, false);
            // merge filter
            params = CoverageUtilities.replaceParameter(params, referenceFilter, ImageMosaicFormat.FILTER);
            // merge USE_JAI_IMAGEREAD to false if needed
            params = CoverageUtilities.replaceParameter(params, ImageMosaicFormat.USE_JAI_IMAGEREAD.getDefaultValue(), ImageMosaicFormat.USE_JAI_IMAGEREAD);
            
            GridGeometry2D gridROI = createGridROI(ciReference, rois, true, referenceCrs);

            if (gridROI != null) {
                params = CoverageUtilities.replaceParameter(params, gridROI,
                        AbstractGridFormat.READ_GRIDGEOMETRY2D);
            }
            
            referenceCoverage = (GridCoverage2D) referenceReader.read(params);

            if (referenceCoverage == null) {
                throw new WPSException("Input Reference Coverage not found");
            }

            if (nowFilter != null) {
                // read now coverage
                readParametersDescriptor = referenceReader.getFormat().getReadParameters();
                // get params for this coverage and override what's needed
                defaultParams = ciReference.getParameters();
                params = CoverageUtils
                        .getParameters(readParametersDescriptor, defaultParams, false);

                // merge filter
                params = CoverageUtilities.replaceParameter(params, nowFilter,
                        ImageMosaicFormat.FILTER);
                // merge USE_JAI_IMAGEREAD to false if needed
                params = CoverageUtilities.replaceParameter(params,
                        ImageMosaicFormat.USE_JAI_IMAGEREAD.getDefaultValue(),
                        ImageMosaicFormat.USE_JAI_IMAGEREAD);
                
                if (gridROI != null) {
                    params = CoverageUtilities.replaceParameter(params, gridROI,
                            AbstractGridFormat.READ_GRIDGEOMETRY2D);
                }
                
                // TODO add tiling, reuse standard values from config
                // TODO add background value, reuse standard values from config
                nowCoverage = (GridCoverage2D) referenceReader.read(params);

                if (nowCoverage == null) {
                    throw new WPSException("Input Current Coverage not found");
                }
            }
            
            // ///////////////////////////////////////////////////////////////
            // Preparing classes for index 3-4
            // ///////////////////////////////////////////////////////////////
            if (index == 3 || index == 4) {
                classes = new TreeSet<Integer>();

                // Selection of the CLC level
                int indexValue = referenceName.indexOf("_L");

                String substring = referenceName.substring(indexValue + 2, indexValue + 3);

                int clcLevel = Integer.parseInt(substring);

                switch (clcLevel) {
                case 1:
                    classes.add(4);
                    break;
                case 2:
                    classes.add(10);
                    classes.add(11);
                    classes.add(14);
                    break;
                case 3:
                    classes.add(1);
                    classes.add(7);
                    classes.add(8);
                    classes.add(10);
                    classes.add(19);
                    classes.add(22);
                    classes.add(31);
                    classes.add(39);
                    classes.add(40);
                    break;
                default:
                    throw new ProcessException("Wrong clc level");
                }
            }
            
            // //////////////////////////////////////////////////////////////////////
            // Logging to WFS ...
            // //////////////////////////////////////////////////////////////////////
            /**
             * Convert the spread attributes into a FeatureType
             */
            List<FeatureAttribute> attributes = new ArrayList<FeatureAttribute>();

            attributes.add(new FeatureAttribute("ftUUID", uuid.toString()));
            attributes.add(new FeatureAttribute("runBegin", new Date()));
            attributes.add(new FeatureAttribute("runEnd", new Date()));
            attributes.add(new FeatureAttribute("itemStatus", "RUNNING"));
            attributes.add(new FeatureAttribute("itemStatusMessage", "Instrumented by Server"));
            attributes.add(new FeatureAttribute("referenceName", referenceName));
            attributes.add(new FeatureAttribute("defaultStyle", defaultStyle));
            attributes.add(new FeatureAttribute("referenceFilter", referenceFilter.toString()));
            attributes.add(new FeatureAttribute("nowFilter", (nowFilter != null ? nowFilter.toString() : "")));
            attributes.add(new FeatureAttribute("index", index));
            attributes.add(new FeatureAttribute("subindex", (subIndex != null ? subIndex : "")));
            attributes.add(new FeatureAttribute("classes", (classes != null ? Arrays.toString(classes.toArray(new Integer[1])) : "")));
            attributes.add(new FeatureAttribute("admUnits", admUnits));
            attributes.add(new FeatureAttribute("admUnitSelectionType", admUnitSelectionType));
            attributes.add(new FeatureAttribute("wsName", wsName));
            attributes.add(new FeatureAttribute("soilIndex", ""));

            features = toFeatureProcess.execute(JTS.toGeometry(ciReference.getNativeBoundingBox()), ciReference.getCRS(), typeName, attributes, null);

            if (features == null || features.isEmpty()) {
                throw new ProcessException("There was an error while converting attributes into FeatureType.");
            }

            /**
             * LOG into the DB
             */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));
            features = wfsLogProcess.execute(features, typeName, wsName, storeName, filter, true, new NullProgressListener());

            if (features == null || features.isEmpty()) {
                throw new ProcessException(
                        "There was an error while logging FeatureType into the storage.");
            }
            
            // ///////////////////////////////////////////////////////////////
            // Calling CLCProcess
            // ///////////////////////////////////////////////////////////////
            final CLCProcess clcProcess = new CLCProcess();
            
            /*LOGGER.finer("Invocking the CLCProcess with the following parameters: ");
            LOGGER.finer(" --> referenceCoverage: " + referenceCoverage);
            LOGGER.finer(" --> nowCoverage: " + nowCoverage);
            LOGGER.finer(" --> classes: " + classes);
            LOGGER.finer(" --> index: " + index);
            LOGGER.finer(" --> rois(" + rois.size() + ")");
            LOGGER.finer(" --> populations(" + populations.size() + ")");*/
            
            List<StatisticContainer> indexValue = clcProcess.execute(referenceCoverage, nowCoverage, classes, index, null, rois, populations, null, (index != 3 && index != 4));
            
            // ///////////////////////////////////////////////////////////////
            // Preparing the Output Object which will be JSON encoded
            // ///////////////////////////////////////////////////////////////
            SoilSealingDTO soilSealingIndexResult = new SoilSealingDTO();
            
            SoilSealingIndex soilSealingIndex = new SoilSealingIndex(index, subIndex);
            soilSealingIndexResult.setIndex(soilSealingIndex);
            
            double[][] refValues = new double[indexValue.size()][];
            double[][] curValues = (nowFilter == null ? null : new double[indexValue.size()][]);

            int i = 0;
            for (StatisticContainer statContainer : indexValue) {
                refValues[i] = (statContainer.getResultsRef() != null ? statContainer.getResultsRef() : new double[1]);
                if(nowFilter != null) curValues[i] = (statContainer.getResultsNow() != null ? statContainer.getResultsNow() : new double[1]);
                i++;
            }
            
            refValues = cleanUpValues(refValues);
            curValues = cleanUpValues(curValues);
            
            String[] clcLevels = new String[classes.size()];
            i = 0;
            for (Integer clcLevel : classes) {
                clcLevels[i++] = String.valueOf(clcLevel);
            }
            
            SoilSealingOutput soilSealingRefTimeOutput = new SoilSealingOutput(referenceName, (String[]) municipalities.toArray(new String[1]), clcLevels, refValues);
            SoilSealingTime soilSealingRefTime = new SoilSealingTime(((IsEqualsToImpl) referenceFilter).getExpression2().toString(), soilSealingRefTimeOutput);
            soilSealingIndexResult.setRefTime(soilSealingRefTime);

            if (nowFilter != null) {
                SoilSealingOutput soilSealingCurTimeOutput = new SoilSealingOutput(referenceName, (String[]) municipalities.toArray(new String[1]), clcLevels, curValues);
                SoilSealingTime soilSealingCurTime = new SoilSealingTime(((IsEqualsToImpl) nowFilter).getExpression2().toString(), soilSealingCurTimeOutput);
                soilSealingIndexResult.setCurTime(soilSealingCurTime);
            }
            
            // ////////////////////////////////////////////////////////////////////////////
            // Create Vectorial Layers for indexes 3 and 4
            // ////////////////////////////////////////////////////////////////////////////
            switch (index) {
            case 3:
            case 4:
                buildVectorialLayerMap(wsName, storeName, municipalities, indexValue, soilSealingIndexResult, ciReference, defaultStyle);
            }
            
            // //////////////////////////////////////////////////////////////////////
            // Updating WFS ...
            // //////////////////////////////////////////////////////////////////////
            /**
             * Update Feature Attributes and LOG into the DB
             */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

            SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter).toArray(new SimpleFeature[1])[0]);

            // build the feature
            feature.setAttribute("runEnd", new Date());
            feature.setAttribute("itemStatus", "COMPLETED");
            feature.setAttribute("itemStatusMessage", "Soil Sealing Process completed successfully");
            feature.setAttribute("soilIndex", JSONSerializer.toJSON(soilSealingIndexResult).toString());

            ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
            output.add(feature);

            features = wfsLogProcess.execute(output, typeName, wsName, storeName, filter, false, new NullProgressListener());
            
            // //////////////////////////////////////////////////////////////////////
            // Return the computed Soil Sealing Index ...
            // //////////////////////////////////////////////////////////////////////
            return soilSealingIndexResult;
        } catch (Exception e) {

            if (features != null) {
                // //////////////////////////////////////////////////////////////////////
                // Updating WFS ...
                // //////////////////////////////////////////////////////////////////////
                /**
                 * Update Feature Attributes and LOG into the DB
                 */
                filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

                SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter).toArray(new SimpleFeature[1])[0]);

                // build the feature
                feature.setAttribute("runEnd", new Date());
                feature.setAttribute("itemStatus", "FAILED");
                feature.setAttribute("itemStatusMessage", "There was an error while while processing Input parameters: " + e.getMessage());

                ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
                output.add(feature);

                features = wfsLogProcess.execute(output, typeName, wsName, storeName, filter, false, new NullProgressListener());
            }

            throw new WPSException("Could process request ", e);
        } finally {
            // clean up
            if (result != null) {
                ImageUtilities.disposePlanarImageChain(result);
            }
            if (referenceCoverage != null) {
                referenceCoverage.dispose(true);
            }
            if (nowCoverage != null) {
                nowCoverage.dispose(true);
            }
        }
    }

    /**
     * This method creates shapefiles for indexes 3 and 4.
     * Enables than the layer on the map.
     * @param municipalities 
     * 
     * @param refWsName 
     * @param storeName 
     * @param municipalities 
     * @param indexValue
     * @param soilSealingIndexResult
     * @param ciReference 
     * @param defaultStyle 
     * @throws IOException 
     * @throws FactoryException 
     * @throws NoSuchAuthorityCodeException 
     * @throws TransformException 
     * @throws MismatchedDimensionException 
     */
    private void buildVectorialLayerMap(String refWsName, String storeName, List<String> municipalities, List<StatisticContainer> indexValue,
            SoilSealingDTO soilSealingIndexResult, CoverageInfo ciReference, String defaultStyle) throws IOException, NoSuchAuthorityCodeException, FactoryException, MismatchedDimensionException, TransformException {
        
        /**
         * Create a FeatureType
         */
        
        /*
         * We use the DataUtilities class to create a FeatureType that will describe the data in our
         * shapefile.
         * 
         * See also the createFeatureType method below for another, more flexible approach.
         */
        /*final SimpleFeatureType TYPE = DataUtilities.createType("Location",
                "the_geom:Point:srid=4326," + // <- the geometry attribute: Point type
                "name:String," +   // <- a String attribute
                "number:Integer"   // a number attribute
        );*/
        final String refName = soilSealingIndexResult.getRefTime().getOutput().getReferenceName();
        final String shpName = (refName.indexOf(":") < 0 ? refName : refName.substring(refName.indexOf(":")+1));
//        final File shpFolder = (GeoserverDataDirectory.findDataFile("soil_index_shps") != null ? 
//                GeoserverDataDirectory.findDataFile("soil_index_shps") : new File(GeoserverDataDirectory.getGeoserverDataDirectory(), "soil_index_shps"));
//        if (!shpFolder.exists()) shpFolder.mkdirs();
//        final File shpFile = File.createTempFile(shpName, ".shp", shpFolder);
        final SimpleFeatureType TYPE = createFeatureType(shpName + System.nanoTime());
        
        /**
         * Create Features
         */
        
        /*
         * A list to collect features as we create them.
         */
        List<SimpleFeature> features = new ArrayList<SimpleFeature>();
        
        // //
        // GRID TO WORLD preparation from reference
        // //
        final AffineTransform gridToWorldCorner = (AffineTransform) ((GridGeometry2D) ciReference.getGrid()).getGridToCRS2D(PixelOrientation.UPPER_LEFT);
        
        /*
         * GeometryFactory will be used to create the geometry attribute of each feature,
         * using a Multipolygon object for the geometry.
         */
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(TYPE);

        int i = 0;
        for (StatisticContainer value : indexValue) {
            if (value.getGeom() instanceof MultiPolygon) {
                featureBuilder.add(JTS.transform(value.getGeom(), new AffineTransform2D(gridToWorldCorner)));
            } else {
                featureBuilder.add(JTS.transform(geometryFactory.createMultiPolygon(new Polygon[] {(Polygon) value.getGeom()}), new AffineTransform2D(gridToWorldCorner)));
            }
            featureBuilder.add(municipalities.get(i));
            featureBuilder.add(value.getResults()[0]);
            featureBuilder.add(value.getColor().getValue());
            SimpleFeature feature = featureBuilder.buildFeature(null);
            features.add(feature);
            i++;
        }
        
//        /**
//         * Create a shapeFile from a FeatureCollection
//         */
//        
//        /*
//         * Get an output file name and create the new shapefile
//         */
//        ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
//
//        Map<String, Serializable> params = new HashMap<String, Serializable>();
//        params.put("url", shpFile.toURI().toURL());
//        params.put("create spatial index", Boolean.TRUE);
//
//        ShapefileDataStore newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
//
//        /*
//         * TYPE is used as a template to describe the file contents
//         */
//        newDataStore.createSchema(TYPE);
//        
//        /**
//         * Write the feature data to the shapefile
//         */
//        
//        /*
//         * Write the features to the shapefile
//         */
//        Transaction transaction = new DefaultTransaction("create");
//
//        String typeName = newDataStore.getTypeNames()[0];
//        SimpleFeatureSource featureSource = newDataStore.getFeatureSource(typeName);
//        SimpleFeatureType SHAPE_TYPE = featureSource.getSchema();
//        /*
//         * The Shapefile format has a couple limitations:
//         * - "the_geom" is always first, and used for the geometry attribute name
//         * - "the_geom" must be of type Point, MultiPoint, MuiltiLineString, MultiPolygon
//         * - Attribute names are limited in length 
//         * - Not all data types are supported (example Timestamp represented as Date)
//         * 
//         * Each data store has different limitations so check the resulting SimpleFeatureType.
//         */
//        LOGGER.fine("SHAPE:"+SHAPE_TYPE);
//
//        if (featureSource instanceof SimpleFeatureStore) {
//            SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
//            /*
//             * SimpleFeatureStore has a method to add features from a
//             * SimpleFeatureCollection object, so we use the ListFeatureCollection
//             * class to wrap our list of features.
//             */
//            SimpleFeatureCollection collection = new ListFeatureCollection(TYPE, features);
//            featureStore.setTransaction(transaction);
//            try {
//                featureStore.addFeatures(collection);
//                transaction.commit();
//            } catch (Exception problem) {
//                problem.printStackTrace();
//                transaction.rollback();
//            } finally {
//                transaction.close();
//            }
//        } else {
//            throw new IOException(typeName + " does not support read/write access");
//        }
        
        /**
         * Import the Feature Collection as a new Layer
         */
        final String layerName = shpName + System.nanoTime();
        ImportProcess importProcess = new ImportProcess(catalog);
        importProcess.execute(DataUtilities.collection(features), null, refWsName, storeName, layerName, ciReference.getCRS(), null, defaultStyle);
        
        soilSealingIndexResult.getRefTime().getOutput().setLayerName(refWsName + ":" + layerName);
    }

    /**
     * Here is how you can use a SimpleFeatureType builder to create the schema for your shapefile
     * dynamically.
     * <p>
     * This method is an improvement on the code used in the main method above (where we used
     * DataUtilities.createFeatureType) because we can set a Coordinate Reference System for the
     * FeatureType and a a maximum field length for the 'name' field
     */
    private static SimpleFeatureType createFeatureType(String name) {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName(name);
        builder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate reference system

        // add attributes in order
        builder.add("the_geom", MultiPolygon.class);
        builder.length(50).add("au_name", String.class); // <- 15 chars width for name field
        builder.add("value", Double.class);
        builder.add("legend", Double.class);
        
        // build the type
        final SimpleFeatureType SOIL_INDEX_TYPE = builder.buildFeatureType();

        return SOIL_INDEX_TYPE;
    }
    
    
}