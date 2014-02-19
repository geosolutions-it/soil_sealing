/* Copyright (c) 2001 - 2014 OpenPlans - www.openplans.org. All rights 
 * reserved. This code is licensed under the GPL 2.0 license, available at the 
 * root application directory.
 */
package org.geoserver.wps.gs.soilsealing;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.media.jai.RenderedOp;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.data.util.CoverageUtils;
import org.geoserver.wps.WPSException;
import org.geoserver.wps.gs.ToFeature;
import org.geoserver.wps.gs.WFSLog;
import org.geoserver.wps.gs.soilsealing.dto.SoilSealingDTO;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.gce.imagemosaic.ImageMosaicFormat;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.resources.image.ImageUtilities;
import org.geotools.util.NullProgressListener;
import org.opengis.coverage.grid.GridCoverageReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValueGroup;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * Middleware process collecting the inputs for {@link CLCProcess} indexes.
 * 
 * @author geosolutions
 * 
 */
public class SoilSealingCLCProcess implements GSProcess {

    /**
     * Geometry and Filter Factories
     */
    private static final FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(
            new PrecisionModel());

    /**
     * The GeoServer Catalog
     */
    private Catalog catalog;

    /**
     * The GeoServer Bean
     */
    private GeoServer geoserver;

    /**
     * Default Constructor
     * 
     * @param catalog
     * @param geoserver
     */
    public SoilSealingCLCProcess(Catalog catalog, GeoServer geoserver) {
        this.catalog = catalog;
        this.geoserver = geoserver;
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
            @DescribeParameter(name = "nowFilter", description = "Filter to use on the raster data", min = 1) Filter nowFilter,
            @DescribeParameter(name = "classes", collectionType = Integer.class, min = 1, description = "The domain of the classes used in input rasters") Set<Integer> classes,
            @DescribeParameter(name = "admUnits", min = 1, description = "Comma Separated list of Administrative Units") List<String> admUnits,
            @DescribeParameter(name = "admUnitSelectionType", min = 1, description = "Administrative Units Slection Type") Integer admUnitSelectionType)
            throws IOException {

        // get the original Coverages
        CoverageInfo ciReference = catalog.getCoverageByName(referenceName);
        if (ciReference == null) {
            throw new WPSException("Could not find coverage " + referenceName);
        }

        // ///////////////////////////////////////////////
        // ChangeMatrix outcome variables ...
        RenderedOp result = null;
        GridCoverage2D nowCoverage = null;
        GridCoverage2D referenceCoverage = null;
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
            
            // read reference coverage
            GridCoverageReader referenceReader = ciReference.getGridCoverageReader(null, null);
            ParameterValueGroup readParametersDescriptor = referenceReader.getFormat()
                    .getReadParameters();

            // get params for this coverage and override what's needed
            Map<String, Serializable> defaultParams = ciReference.getParameters();
            GeneralParameterValue[] params = CoverageUtils.getParameters(readParametersDescriptor,
                    defaultParams, false);
            // merge filter
            params = CoverageUtilities.replaceParameter(params, referenceFilter, ImageMosaicFormat.FILTER);
            // merge USE_JAI_IMAGEREAD to false if needed
            params = CoverageUtilities.replaceParameter(params,
                    ImageMosaicFormat.USE_JAI_IMAGEREAD.getDefaultValue(),
                    ImageMosaicFormat.USE_JAI_IMAGEREAD);

            referenceCoverage = (GridCoverage2D) referenceReader.read(params);

            if (referenceCoverage == null) {
                throw new WPSException("Input Reference Coverage not found");
            }

            // read now coverage
            readParametersDescriptor = referenceReader.getFormat().getReadParameters();
            // get params for this coverage and override what's needed
            defaultParams = ciReference.getParameters();
            params = CoverageUtils.getParameters(readParametersDescriptor, defaultParams, false);

            // merge filter
            params = CoverageUtilities.replaceParameter(params, nowFilter, ImageMosaicFormat.FILTER);
            // merge USE_JAI_IMAGEREAD to false if needed
            params = CoverageUtilities.replaceParameter(params,
                    ImageMosaicFormat.USE_JAI_IMAGEREAD.getDefaultValue(),
                    ImageMosaicFormat.USE_JAI_IMAGEREAD);

            // TODO add tiling, reuse standard values from config
            // TODO add background value, reuse standard values from config
            nowCoverage = (GridCoverage2D) referenceReader.read(params);

            if (nowCoverage == null) {
                throw new WPSException("Input Current Coverage not found");
            }
            
            return null;
        } catch (Exception e) {

//            if (features != null) {
//                // //////////////////////////////////////////////////////////////////////
//                // Updating WFS ...
//                // //////////////////////////////////////////////////////////////////////
//                /**
//                 * Update Feature Attributes and LOG into the DB
//                 */
//                filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));
//
//                SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter)
//                        .toArray(new SimpleFeature[1])[0]);
//
//                // build the feature
//                feature.setAttribute("runEnd", new Date());
//                feature.setAttribute("itemStatus", "FAILED");
//                feature.setAttribute(
//                        "itemStatusMessage",
//                        "There was an error while while processing Input parameters: "
//                                + e.getMessage());
//
//                ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
//                output.add(feature);
//
//                features = wfsLogProcess.execute(output, typeName, wsName, storeName, filter,
//                        false, new NullProgressListener());
//            }

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