/* Copyright (c) 2001 - 2014 OpenPlans - www.openplans.org. All rights 
 * reserved. This code is licensed under the GPL 2.0 license, available at the 
 * root application directory.
 */
package org.geoserver.wps.gs.soilsealing;

import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.IsEqualsToImpl;
import org.geotools.gce.imagemosaic.ImageMosaicFormat;
import org.geotools.geometry.jts.JTS;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.resources.image.ImageUtilities;
import org.geotools.util.NullProgressListener;
import org.geotools.util.logging.Logging;
import org.opengis.coverage.grid.GridCoverageReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.geometry.Envelope;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Middleware process collecting the inputs for {@link UrbanGridProcess} indexes.
 * 
 * @author geosolutions
 * 
 */
@DescribeProcess(title = "SoilSealingImperviousness", description = "Middleware process collecting the inputs for UrbanGridProcess indexes")
public class SoilSealingImperviousnessProcess extends SoilSealingMiddlewareProcess {

    private static final double INDEX_10_VALUE = 6.0;

    private final static Logger LOGGER = Logging.getLogger(SoilSealingImperviousnessProcess.class);

    /**
     * Default Constructor
     * 
     * @param catalog
     * @param geoserver
     */
    public SoilSealingImperviousnessProcess(Catalog catalog, GeoServer geoserver) {
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
    @DescribeResult(name = "soilSealingImperviousness", description = "SoilSealing Imperviousness Middleware Process", type = SoilSealingDTO.class)
    public SoilSealingDTO execute(
            @DescribeParameter(name = "name", description = "Name of the raster, optionally fully qualified (workspace:name)") String referenceName,
            @DescribeParameter(name = "defaultStyle", description = "Name of the raster default style") String defaultStyle,
            @DescribeParameter(name = "storeName", description = "Name of the destination data store to log info") String storeName,
            @DescribeParameter(name = "typeName", description = "Name of the destination feature type to log info") String typeName,
            @DescribeParameter(name = "referenceFilter", description = "Filter to use on the raster data", min = 1) Filter referenceFilter,
            @DescribeParameter(name = "nowFilter", description = "Filter to use on the raster data", min = 0) Filter nowFilter,
            @DescribeParameter(name = "index", min = 1, description = "Index to calculate") int index,
            @DescribeParameter(name = "subindex", min = 0, description = "String indicating which sub-index must be calculated {a,b,c}") String subIndex,
            @DescribeParameter(name = "geocoderLayer", min = 1, description = "Name of the geocoder layer, optionally fully qualified (workspace:name)") String geocoderLayer,
            @DescribeParameter(name = "geocoderPopulationLayer", min = 1, description = "Name of the geocoder population layer, optionally fully qualified (workspace:name)") String geocoderPopulationLayer,
            @DescribeParameter(name = "imperviousnessLayer", min = 1, description = "Name of the imperviousness layer, optionally fully qualified (workspace:name)") String imperviousnessLayer,
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

        FeatureTypeInfo imperviousnessReference = catalog.getFeatureTypeByName(imperviousnessLayer);
        if (imperviousnessReference == null) {
            throw new WPSException("Could not find imperviousness reference layer ("
                    + imperviousnessLayer + ")");
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
        if (nowFilter != null)
            populations.add(new LinkedList<Integer>());
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
            final String referenceYear = ((IsEqualsToImpl) referenceFilter).getExpression2()
                    .toString().substring(0, 4);
            final String currentYear = (nowFilter == null ? null : ((IsEqualsToImpl) nowFilter)
                    .getExpression2().toString().substring(0, 4));

            // //////////////////////////////////////
            // Scan the geocoding layers and prepare
            // the geometries and population values.
            // //////////////////////////////////////
            boolean toRasterSpace = false;

            switch (index) {
            case 7:
                if (!subIndex.equals("a"))
                    break;
            case 8:
            case 9:
            case 10:
                toRasterSpace = true;
            }

            final CoordinateReferenceSystem referenceCrs = ciReference.getCRS();
            prepareAdminROIs(nowFilter, admUnits, admUnitSelectionType, ciReference,
                    geoCodingReference, populationReference, municipalities, rois, populations,
                    referenceYear, currentYear, referenceCrs, toRasterSpace);

            // read reference coverage
            GridCoverageReader referenceReader = ciReference.getGridCoverageReader(null, null);
            ParameterValueGroup readParametersDescriptor = referenceReader.getFormat()
                    .getReadParameters();

            // get params for this coverage and override what's needed
            Map<String, Serializable> defaultParams = ciReference.getParameters();
            GeneralParameterValue[] params = CoverageUtils.getParameters(readParametersDescriptor,
                    defaultParams, false);
            // merge filter
            params = CoverageUtilities.replaceParameter(params, referenceFilter,
                    ImageMosaicFormat.FILTER);
            // merge USE_JAI_IMAGEREAD to false if needed
            params = CoverageUtilities.replaceParameter(params,
                    ImageMosaicFormat.USE_JAI_IMAGEREAD.getDefaultValue(),
                    ImageMosaicFormat.USE_JAI_IMAGEREAD);
            
            // Creation of a GridGeometry object used for forcing the reader to read only the active zones
            GridGeometry2D gridROI = createGridROI(ciReference, rois, toRasterSpace, referenceCrs);

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
            attributes.add(new FeatureAttribute("index", getSealingIndex(index)));
            attributes.add(new FeatureAttribute("subindex", (subIndex != null ? getSealingSubIndex(subIndex) : "")));
            attributes.add(new FeatureAttribute("classes", ""));
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
            // Calling UrbanGridProcess
            // ///////////////////////////////////////////////////////////////
            final UrbanGridProcess urbanGridProcess = new UrbanGridProcess(imperviousnessReference,
                    referenceYear, currentYear);

            List<StatisticContainer> indexValue = urbanGridProcess.execute(referenceCoverage,
                    nowCoverage, index, subIndex, null, rois, populations,
                    (index == 10 ? INDEX_10_VALUE : null));

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
                refValues[i] = (statContainer.getResultsRef() != null ? statContainer
                        .getResultsRef() : new double[1]);
                if (nowFilter != null)
                    curValues[i] = (statContainer.getResultsNow() != null ? statContainer
                            .getResultsNow() : new double[1]);
                i++;
            }

            refValues = cleanUpValues(refValues);
            curValues = cleanUpValues(curValues);

            String[] clcLevels = null;

            SoilSealingOutput soilSealingRefTimeOutput = new SoilSealingOutput(referenceName,
                    (String[]) municipalities.toArray(new String[1]), clcLevels, refValues);
            SoilSealingTime soilSealingRefTime = new SoilSealingTime(
                    ((IsEqualsToImpl) referenceFilter).getExpression2().toString(),
                    soilSealingRefTimeOutput);
            soilSealingIndexResult.setRefTime(soilSealingRefTime);

            if (nowFilter != null) {
                SoilSealingOutput soilSealingCurTimeOutput = new SoilSealingOutput(referenceName,
                        (String[]) municipalities.toArray(new String[1]), clcLevels, curValues);
                SoilSealingTime soilSealingCurTime = new SoilSealingTime(
                        ((IsEqualsToImpl) nowFilter).getExpression2().toString(),
                        soilSealingCurTimeOutput);
                soilSealingIndexResult.setCurTime(soilSealingCurTime);
            }
            
            if (index == 8) {
                buildRasterMap(soilSealingIndexResult, indexValue, referenceCoverage, wsName,
                        defaultStyle);
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
     * 
     * @param index
     * @return
     */
    private String getSealingIndex(int index) {
        String indexName = "";
        
        switch(index) {
        case 1:
            indexName = "Coverage coefficient";
            break;
        case 2:
            indexName = "Rate of Change";
            break;
        case 3:
            indexName = "Marginal Land Take";
            break;
        case 4:
            indexName = "Urban Sprawl Indicator";
            break;
        case 5:
            indexName = "Urban Dispersion";
            break;
        case 6:
            indexName = "Edge Density";
            break;
        case 7:
            indexName = "Dispersive Urban Growth";
            break;
        case 8:
            indexName = "Fragmentation";
            break;
        case 9:
            indexName = "Land Take";
            break;
        case 10:
            indexName = "Environmental Impact of Land Take";
            break;
        }
        
        return indexName;
    }
    
    /**
     * 
     * @param index
     * @return
     */
    private String getSealingSubIndex(String index) {
        String indexName = "";
        
        if(index.equals("a"))
            indexName = "Urban Area";
        else if(index.equals("b"))
            indexName = "Highest Polygon Ratio";
        else if(index.equals("c"))
            indexName = "Other Polygons Ratio";
        
        return indexName;
    }
    
    /**
     * 
     * @param soilSealingIndexResult
     * @param indexValue
     * @param inputCov
     * @param refWsName
     * @param defaultStyle
     */
    private void buildRasterMap(SoilSealingDTO soilSealingIndexResult,
            List<StatisticContainer> indexValue, GridCoverage2D inputCov, String refWsName,
            String defaultStyle) {

        StatisticContainer container = indexValue.get(0);

        // Selection of the images
        RenderedImage referenceImage = container.getReferenceImage();

        RenderedImage nowImage = container.getNowImage();

        RenderedImage diffImage = container.getDiffImage();

        // Selection of the names associated to the reference and current times
        String time = "" + System.nanoTime();

        final String rasterRefName = "referenceUrbanGrids";
        final String rasterCurName = "currentUrbanGrids";
        final String rasterDiffName = "diffUrbanGrids";
        // Creation of the final Coverages
        // hints for tiling
        final Hints hints = GeoTools.getDefaultHints().clone();

        GridCoverageFactory factory = new GridCoverageFactory(hints);

        Envelope envelope = inputCov.getEnvelope();

        GridCoverage2D reference = factory.create(rasterRefName + time, referenceImage, envelope);

        GridCoverage2D now = null;

        GridCoverage2D diff = null;
        // Creation of the coverages for the current and difference images
        if (nowImage != null) {
            if (diffImage == null) {
                throw new ProcessException("Unable to calculate the difference image for index 8");
            }

            now = factory.create(rasterCurName + time, nowImage, envelope);

            diff = factory.create(rasterDiffName + time, diffImage, envelope);
        }

        /**
         * Import the GridCoverages as new Layers
         */
        // Importing of the Reference Coverage
        ImportProcess importProcess = new ImportProcess(catalog);
        importProcess.execute(null, reference, refWsName, null, reference.getName().toString(),
                reference.getCoordinateReferenceSystem(), null, defaultStyle);

        soilSealingIndexResult.getRefTime().getOutput()
                .setLayerName(refWsName + ":" + reference.getName().toString());

        // Importing of the current and difference coverages if present
        if (now != null) {
            // Current coverage
            importProcess.execute(null, now, refWsName, null, now.getName().toString(),
                    now.getCoordinateReferenceSystem(), null, defaultStyle);

            soilSealingIndexResult.getCurTime().getOutput()
                    .setLayerName(refWsName + ":" + now.getName().toString());

            // Difference coverage
            importProcess.execute(null, diff, refWsName, null, diff.getName().toString(),
                    diff.getCoordinateReferenceSystem(), null, defaultStyle);

            soilSealingIndexResult.setDiffImageName(refWsName + ":" + diff.getName().toString());
        }
    }
}
