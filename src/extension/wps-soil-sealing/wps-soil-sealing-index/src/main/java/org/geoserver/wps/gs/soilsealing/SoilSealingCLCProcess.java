/* Copyright (c) 2001 - 2014 OpenPlans - www.openplans.org. All rights 
 * reserved. This code is licensed under the GPL 2.0 license, available at the 
 * root application directory.
 */
package org.geoserver.wps.gs.soilsealing;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import javax.media.jai.RenderedOp;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.data.util.CoverageUtils;
import org.geoserver.wps.WPSException;
import org.geoserver.wps.gs.ToFeature;
import org.geoserver.wps.gs.WFSLog;
import org.geoserver.wps.gs.soilsealing.CLCProcess.StatisticContainer;
import org.geoserver.wps.gs.soilsealing.SoilSealingAdministrativeUnit.AuSelectionType;
import org.geoserver.wps.gs.soilsealing.model.SoilSealingIndex;
import org.geoserver.wps.gs.soilsealing.model.SoilSealingOutput;
import org.geoserver.wps.gs.soilsealing.model.SoilSealingTime;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.filter.IsEqualsToImpl;
import org.geotools.gce.imagemosaic.ImageMosaicFormat;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.resources.image.ImageUtilities;
import org.geotools.util.logging.Logging;
import org.opengis.coverage.grid.GridCoverageReader;
import org.opengis.filter.Filter;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;

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
            @DescribeParameter(name = "classes", collectionType = Integer.class, min = 1, description = "The domain of the classes used in input rasters") Set<Integer> classes,
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
//          TODO
//          if (gridROI != null) {
//              params = CoverageUtilities.replaceParameter(params, gridROI,
//                      AbstractGridFormat.READ_GRIDGEOMETRY2D);
//          }
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
//                TODO
//                if (gridROI != null) {
//                    params = CoverageUtilities.replaceParameter(params, gridROI,
//                            AbstractGridFormat.READ_GRIDGEOMETRY2D);
//                }
                // TODO add tiling, reuse standard values from config
                // TODO add background value, reuse standard values from config
                nowCoverage = (GridCoverage2D) referenceReader.read(params);

                if (nowCoverage == null) {
                    throw new WPSException("Input Current Coverage not found");
                }
            }
            
            // ///////////////////////////////////////////////////////////////
            // Calling CLCProcess
            // ///////////////////////////////////////////////////////////////
            final CLCProcess clcProcess = new CLCProcess();
            
            LOGGER.finer("Invocking the CLCProcess with the following parameters: ");
            LOGGER.finer(" --> referenceCoverage: " + referenceCoverage);
            LOGGER.finer(" --> nowCoverage: " + nowCoverage);
            LOGGER.finer(" --> classes: " + classes);
            LOGGER.finer(" --> index: " + index);
            LOGGER.finer(" --> rois(" + rois.size() + ")");
            LOGGER.finer(" --> populations(" + populations.size() + ")");
            
            List<StatisticContainer> indexValue = clcProcess.execute(referenceCoverage, nowCoverage, classes, index, null, rois, populations, null, true);
            
            // ///////////////////////////////////////////////////////////////
            // Preparing the Output Object which will be JSON encoded
            // ///////////////////////////////////////////////////////////////
            SoilSealingDTO soilSealingIndexResult = new SoilSealingDTO();
            
            SoilSealingIndex soilSealingIndex = new SoilSealingIndex(index, "");
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
            
            return soilSealingIndexResult;
        } catch (Exception e) {

            // if (features != null) {
            // // //////////////////////////////////////////////////////////////////////
            // // Updating WFS ...
            // // //////////////////////////////////////////////////////////////////////
            // /**
            // * Update Feature Attributes and LOG into the DB
            // */
            // filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));
            //
            // SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter)
            // .toArray(new SimpleFeature[1])[0]);
            //
            // // build the feature
            // feature.setAttribute("runEnd", new Date());
            // feature.setAttribute("itemStatus", "FAILED");
            // feature.setAttribute(
            // "itemStatusMessage",
            // "There was an error while while processing Input parameters: "
            // + e.getMessage());
            //
            // ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
            // output.add(feature);
            //
            // features = wfsLogProcess.execute(output, typeName, wsName, storeName, filter,
            // false, new NullProgressListener());
            // }

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

}