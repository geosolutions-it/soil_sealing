/* Copyright (c) 2001 - 2014 OpenPlans - www.openplans.org. All rights 
 * reserved. This code is licensed under the GPL 2.0 license, available at the 
 * root application directory.
 */
package org.geoserver.wps.gs.soilsealing;

import it.geosolutions.jaiext.stats.Statistics;
import it.geosolutions.jaiext.stats.Statistics.StatsType;
import it.geosolutions.jaiext.stats.StatisticsDescriptor;

import java.awt.geom.AffineTransform;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import javax.media.jai.RenderedOp;

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
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.filter.IsEqualsToImpl;
import org.geotools.gce.imagemosaic.ImageMosaicFormat;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.raster.CropCoverage;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.resources.image.ImageUtilities;
import org.geotools.util.logging.Logging;
import org.opengis.coverage.grid.GridCoverageReader;
import org.opengis.filter.Filter;
import org.opengis.geometry.Envelope;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;

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

            // TODO
            // if (gridROI != null) {
            // params = CoverageUtilities.replaceParameter(params, gridROI,
            // AbstractGridFormat.READ_GRIDGEOMETRY2D);
            // }

            // Creation of a Geometry union for cropping the input coverages
            Geometry union = null;

            CoordinateReferenceSystem covCRS = referenceCrs;

            final AffineTransform gridToWorldCorner = (AffineTransform) ((GridGeometry2D) ciReference
                    .getGrid()).getGridToCRS2D(PixelOrientation.UPPER_LEFT);

            // Union of all the Geometries
            for (Geometry geo : rois) {
                if (union == null) {
                    if (toRasterSpace) {
                        union = JTS.transform(geo, ProjectiveTransform.create(gridToWorldCorner));
                    } else {
                        union = geo;
                    }
                } else {
                    if (toRasterSpace) {
                        Geometry projected = JTS.transform(geo,
                                ProjectiveTransform.create(gridToWorldCorner));
                        union.union(projected);
                    } else {
                        union.union(geo);
                    }
                }
            }
            // Setting of the final srID and reproject to the final CRS
            CoordinateReferenceSystem crs = (CoordinateReferenceSystem) union.getUserData();
            if (crs != null) {
                MathTransform trans = CRS.findMathTransform(crs, covCRS);
                union = JTS.transform(union, trans);
                union.setUserData(covCRS);
            }

            if (union.getSRID() == 0) {
                int srIDfinal = CRS.lookupEpsgCode(covCRS, true);
                union.setSRID(srIDfinal);
            }

            if (union.getUserData() == null) {
                union.setUserData(covCRS);
            }
            // Creation of a GridGeometry object used for forcing the reader to read only the active zones
            GridGeometry2D gridROI = null;

            if (!union.isEmpty()) {
                
                //
                // Make sure the provided area intersects the layer BBOX in the layer CRS
                //
                final ReferencedEnvelope crsBBOX = ciReference.boundingBox();
                union = union.intersection(JTS.toGeometry(crsBBOX));
                if (union.isEmpty()) {
                    throw new WPSException(
                            "The provided Administrative Areas does not intersect the reference data BBOX: ",
                            union.toText());
                }
                
                com.vividsolutions.jts.geom.Envelope envelope = union.getEnvelopeInternal();
                // create with supplied crs
                Envelope2D bounds = JTS.getEnvelope2D(envelope, covCRS);

                // Creation of a GridGeometry2D instance used for cropping the input images
                gridROI = new GridGeometry2D(PixelInCell.CELL_CORNER,
                        (MathTransform) gridToWorldCorner, bounds, null);
            }

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

                // TODO
                // if (gridROI != null) {
                // params = CoverageUtilities.replaceParameter(params, gridROI,
                // AbstractGridFormat.READ_GRIDGEOMETRY2D);
                // }
                // TODO add tiling, reuse standard values from config
                // TODO add background value, reuse standard values from config
                nowCoverage = (GridCoverage2D) referenceReader.read(params);

                if (nowCoverage == null) {
                    throw new WPSException("Input Current Coverage not found");
                }
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

    private void buildRasterMap(SoilSealingDTO soilSealingIndexResult,
            List<StatisticContainer> indexValue, GridCoverage2D inputCov, String refWsName,
            String defaultStyle) {

        StatisticContainer container = indexValue.get(0);

        // Selection of the images
        RenderedImage referenceImage = container.getReferenceImage();
        //
        // StatsType[] stats = new StatsType[]{StatsType.MAX};
        //
        // RenderedOp test = StatisticsDescriptor.create(inputCov.getRenderedImage(), 1, 1, null, null, false, new int[]{0}, stats, null);
        //
        // double mean = (Double) ((Statistics[][])test.getProperty(Statistics.STATS_PROPERTY))[0][0].getResult();

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
