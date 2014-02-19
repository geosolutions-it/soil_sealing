/* Copyright (c) 2001 - 2014 OpenPlans - www.openplans.org. All rights 
 * reserved. This code is licensed under the GPL 2.0 license, available at the 
 * root application directory.
 */
package org.geoserver.extension;

import it.geosolutions.jaiext.changematrix.ChangeMatrixDescriptor;
import it.geosolutions.jaiext.changematrix.ChangeMatrixDescriptor.ChangeMatrix;
import it.geosolutions.jaiext.changematrix.ChangeMatrixRIF;

import java.awt.Point;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import javax.media.jai.JAI;
import javax.media.jai.ParameterBlockJAI;
import javax.media.jai.ROI;
import javax.media.jai.ROIShape;
import javax.media.jai.RenderedOp;

import net.sf.json.JSONSerializer;

import org.apache.commons.io.FileUtils;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.config.CoverageAccessInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.data.util.CoverageUtils;
import org.geoserver.wps.WPSException;
import org.geoserver.wps.gs.CoverageImporter;
import org.geoserver.wps.gs.ImportProcess;
import org.geoserver.wps.gs.ToFeature;
import org.geoserver.wps.gs.WFSLog;
import org.geoserver.wps.ppio.FeatureAttribute;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.DataSourceException;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.gce.imagemosaic.ImageMosaicFormat;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.LiteShape2;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.image.jai.Registry;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.process.raster.CoverageUtilities;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.resources.image.ImageUtilities;
import org.geotools.util.NullProgressListener;
import org.geotools.utils.imageoverviews.OverviewsEmbedder;
import org.geotools.utils.progress.ExceptionEvent;
import org.geotools.utils.progress.ProcessingEvent;
import org.geotools.utils.progress.ProcessingEventListener;
import org.jaitools.imageutils.ROIGeometry;
import org.opengis.coverage.grid.GridCoverageReader;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterDescriptor;
import org.opengis.parameter.ParameterValue;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * A process that returns a coverage fully (something which is un-necessarily hard in WCS)
 * 
 * @author Simone Giannecchini, GeoSolutions SAS
 * @author Andrea Aime, GeoSolutions SAS
 */
@DescribeProcess(title = "ChangeMatrix", description = "Compute the ChangeMatrix between two coverages")
public class ChangeMatrixProcess implements GSProcess {

    static {
        Registry.registerRIF(JAI.getDefaultInstance(), new ChangeMatrixDescriptor(),
                new ChangeMatrixRIF(), Registry.JAI_TOOLS_PRODUCT);
    }

    private final static boolean DEBUG = Boolean.getBoolean("org.geoserver.wps.debug");

    private static final int PIXEL_MULTY_ARG_INDEX = 100;

    private static final FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(
            new PrecisionModel());

    private Catalog catalog;

    private GeoServer geoserver;

    public ChangeMatrixProcess(Catalog catalog, GeoServer geoserver) {
        this.catalog = catalog;
        this.geoserver = geoserver;
    }

    /**
     * @param classes representing the domain of the classes (Mandatory, not empty)
     * @param rasterT0 that is the reference Image (Mandatory)
     * @param rasterT1 rasterT1 that is the update situation (Mandatory)
     * @param roi that identifies the optional ROI (so that could be null)
     * @return
     */
    @DescribeResult(name = "changeMatrix", description = "the ChangeMatrix", type = ChangeMatrixDTO.class)
    public ChangeMatrixDTO execute(
            @DescribeParameter(name = "name", description = "Name of the raster, optionally fully qualified (workspace:name)") String referenceName,
            @DescribeParameter(name = "defaultStyle", description = "Name of the raster default style") String defaultStyle,
            @DescribeParameter(name = "storeName", description = "Name of the destination data store to log info") String storeName,
            @DescribeParameter(name = "typeName", description = "Name of the destination feature type to log info") String typeName,
            @DescribeParameter(name = "referenceFilter", description = "Filter to use on the raster data", min = 1) Filter referenceFilter,
            @DescribeParameter(name = "nowFilter", description = "Filter to use on the raster data", min = 1) Filter nowFilter,
            @DescribeParameter(name = "classes", collectionType = Integer.class, min = 1, description = "The domain of the classes used in input rasters") Set<Integer> classes,
            @DescribeParameter(name = "ROI", min = 0, description = "Region Of Interest") Geometry roi)
            throws IOException {

        // DEBUG OPTION
        if (DEBUG) {
            return getTestMap();
        }

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

            // now perform the operation
            final ChangeMatrix cm = new ChangeMatrix(classes);
            final ParameterBlockJAI pbj = new ParameterBlockJAI("ChangeMatrix");

            pbj.setParameter("result", cm);
            pbj.setParameter(
                    ChangeMatrixDescriptor.PARAM_NAMES[ChangeMatrixDescriptor.PIXEL_MULTY_ARG_INDEX],
                    PIXEL_MULTY_ARG_INDEX);

            GridGeometry2D gridROI = null;

            // handle Region Of Interest
            if (roi != null) {
                if (roi instanceof GeometryCollection) {
                    List<Polygon> geomPolys = new ArrayList<Polygon>();
                    for (int g = 0; g < ((GeometryCollection) roi).getNumGeometries(); g++) {
                        extractPolygons(geomPolys, ((GeometryCollection) roi).getGeometryN(g));
                    }

                    if (geomPolys.size() == 0) {
                        roi = GEOMETRY_FACTORY.createPolygon(null, null);
                    } else if (geomPolys.size() == 1) {
                        roi = geomPolys.get(0);
                    } else {
                        roi = roi.getFactory().createMultiPolygon(
                                geomPolys.toArray(new Polygon[geomPolys.size()]));
                    }
                }

                //
                // Make sure the provided roi intersects the layer BBOX in wgs84
                //
                final ReferencedEnvelope wgs84BBOX = ciReference.getLatLonBoundingBox();
                roi = roi.intersection(JTS.toGeometry(wgs84BBOX));
                if (roi.isEmpty()) {
                    throw new WPSException(
                            "The provided ROI does not intersect the reference data BBOX: ",
                            roi.toText());
                }

                // Geometry associated with the ROI
                Geometry roiPrj = null;

                //
                // GRID TO WORLD preparation from reference
                //
                final AffineTransform gridToWorldCorner = (AffineTransform) ((GridGeometry2D) ciReference
                        .getGrid()).getGridToCRS2D(PixelOrientation.UPPER_LEFT);

                // check if we need to reproject the ROI from WGS84 (standard in the input) to the reference CRS
                final CoordinateReferenceSystem crs = ciReference.getCRS();
                if (CRS.equalsIgnoreMetadata(crs, DefaultGeographicCRS.WGS84)) {
                    roiPrj = roi;
                    pbj.setParameter("ROI", CoverageUtilities.prepareROI(roi, gridToWorldCorner));
                } else {
                    // reproject
                    MathTransform transform = CRS.findMathTransform(DefaultGeographicCRS.WGS84,
                            crs, true);
                    if (transform.isIdentity()) {
                        roiPrj = roi;
                    } else {
                        roiPrj = JTS.transform(roi, transform);
                    }
                    pbj.setParameter("ROI", prepareROIGeometry(roiPrj, gridToWorldCorner));
                }
                //
                // Make sure the provided area intersects the layer BBOX in the layer CRS
                //
                final ReferencedEnvelope crsBBOX = ciReference.boundingBox();
                roiPrj = roiPrj.intersection(JTS.toGeometry(crsBBOX));
                if (roiPrj.isEmpty()) {
                    throw new WPSException(
                            "The provided ROI does not intersect the reference data BBOX: ",
                            roiPrj.toText());
                }

                // Creation of a GridGeometry object used for forcing the reader
                Envelope envelope = roiPrj.getEnvelopeInternal();
                // create with supplied crs
                Envelope2D bounds = JTS.getEnvelope2D(envelope, crs);
                // Creation of a GridGeometry2D instance used for cropping the input images
                gridROI = new GridGeometry2D(PixelInCell.CELL_CORNER,
                        (MathTransform) gridToWorldCorner, bounds, null);
            }

            // merge filter
            params = replaceParameter(params, referenceFilter, ImageMosaicFormat.FILTER);
            // merge USE_JAI_IMAGEREAD to false if needed
            params = replaceParameter(params,
                    ImageMosaicFormat.USE_JAI_IMAGEREAD.getDefaultValue(),
                    ImageMosaicFormat.USE_JAI_IMAGEREAD);
            if (gridROI != null) {
                params = replaceParameter(params, gridROI, AbstractGridFormat.READ_GRIDGEOMETRY2D);
            }
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
            params = replaceParameter(params, nowFilter, ImageMosaicFormat.FILTER);
            // merge USE_JAI_IMAGEREAD to false if needed
            params = replaceParameter(params,
                    ImageMosaicFormat.USE_JAI_IMAGEREAD.getDefaultValue(),
                    ImageMosaicFormat.USE_JAI_IMAGEREAD);
            if (gridROI != null) {
                params = replaceParameter(params, gridROI, AbstractGridFormat.READ_GRIDGEOMETRY2D);
            }
            // TODO add tiling, reuse standard values from config
            // TODO add background value, reuse standard values from config
            nowCoverage = (GridCoverage2D) referenceReader.read(params);

            if (nowCoverage == null) {
                throw new WPSException("Input Current Coverage not found");
            }

            // Setting of the sources
            pbj.addSource(referenceCoverage.getRenderedImage());
            pbj.addSource(nowCoverage.getRenderedImage());

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
            attributes.add(new FeatureAttribute("nowFilter", nowFilter.toString()));
            attributes.add(new FeatureAttribute("roi", roi.getEnvelope()));
            attributes.add(new FeatureAttribute("wsName", wsName));
            attributes.add(new FeatureAttribute("layerName", ""));
            attributes.add(new FeatureAttribute("changeMatrix", ""));

            features = toFeatureProcess.execute(JTS.toGeometry(ciReference.getNativeBoundingBox()),
                    ciReference.getCRS(), typeName, attributes, null);

            if (features == null || features.isEmpty()) {
                throw new ProcessException(
                        "There was an error while converting attributes into FeatureType.");
            }

            /**
             * LOG into the DB
             */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));
            features = wfsLogProcess.execute(features, typeName, wsName, storeName, filter, true,
                    new NullProgressListener());

            if (features == null || features.isEmpty()) {
                throw new ProcessException(
                        "There was an error while logging FeatureType into the storage.");
            }

            // //////////////////////////////////////////////////////////////////////
            // Compute the Change Matrix ...
            // //////////////////////////////////////////////////////////////////////
            result = JAI.create("ChangeMatrix", pbj, null);

            //
            // result computation
            //
            final int numTileX = result.getNumXTiles();
            final int numTileY = result.getNumYTiles();
            final int minTileX = result.getMinTileX();
            final int minTileY = result.getMinTileY();
            final List<Point> tiles = new ArrayList<Point>(numTileX * numTileY);
            for (int i = minTileX; i < minTileX + numTileX; i++) {
                for (int j = minTileY; j < minTileY + numTileY; j++) {
                    tiles.add(new Point(i, j));
                }
            }
            final CountDownLatch sem = new CountDownLatch(tiles.size());
            // how many JAI tiles do we have?
            final CoverageAccessInfo coverageAccess = geoserver.getGlobal().getCoverageAccess();
            final ThreadPoolExecutor executor = coverageAccess.getThreadPoolExecutor();
            final RenderedOp temp = result;
            for (final Point tile : tiles) {

                executor.execute(new Runnable() {

                    @Override
                    public void run() {
                        temp.getTile(tile.x, tile.y);
                        sem.countDown();
                    }
                });
            }
            try {
                sem.await();
            } catch (InterruptedException e) {
                // TODO handle error
                return null;
            }
            // computation done!
            cm.freeze();

            // //////////////////////////////////////////////////////////////////////
            // Import into GeoServer the new raster 'result' ...
            // //////////////////////////////////////////////////////////////////////
            /**
             * create the final coverage using final envelope
             */
            // hints for tiling
            final Hints hints = GeoTools.getDefaultHints().clone();

            final String rasterName = ciReference.getName() + "_cm_" + System.nanoTime();
            final GridCoverage2D retValue = new GridCoverageFactory(hints).create(rasterName,
                    result, referenceCoverage.getEnvelope());

            /**
             * creating the ChangeMatrix grid
             */
            final ChangeMatrixDTO changeMatrix = new ChangeMatrixDTO(cm, classes, rasterName);

            /**
             * Add Overviews...
             */
            final File file = File.createTempFile(retValue.getName().toString(), ".tif");
            GeoTiffWriter writer = new GeoTiffWriter(file);

            // setting the write parameters for this geotiff
            final ParameterValueGroup gtiffParams = new GeoTiffFormat().getWriteParameters();
            gtiffParams.parameter(AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName().toString())
                    .setValue(CoverageImporter.DEFAULT_WRITE_PARAMS);
            final GeneralParameterValue[] wps = (GeneralParameterValue[]) gtiffParams.values()
                    .toArray(new GeneralParameterValue[1]);

            try {
                writer.write(retValue, wps);
            } finally {
                try {
                    writer.dispose();
                } catch (Exception e) {
                    throw new IOException("Unable to write the output raster.", e);
                }
            }

            AbstractGridCoverage2DReader gtiffReader = null;
            try {
                gtiffReader = new GeoTiffFormat().getReader(file);
                generateOverviews(gtiffReader);
            } catch (DataSourceException e) {
                // we tried, no need to fuss around this one
            }

            /**
             * import the new coverage into the GeoServer catalog
             */
            try {
                ImportProcess importProcess = new ImportProcess(catalog);
                GridCoverage2D retOvValue = gtiffReader.read(wps);
                importProcess.execute(null, retOvValue, wsName, null,
                        retValue.getName().toString(), retValue.getCoordinateReferenceSystem(),
                        null, defaultStyle);
            } finally {
                if (gtiffReader != null) {
                    gtiffReader.dispose();
                }

                try {
                    FileUtils.forceDelete(file);
                } catch (Exception e) {
                    // we tried, no need to fuss around this one
                }
            }

            // //////////////////////////////////////////////////////////////////////
            // Updating WFS ...
            // //////////////////////////////////////////////////////////////////////
            /**
             * Update Feature Attributes and LOG into the DB
             */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

            SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter)
                    .toArray(new SimpleFeature[1])[0]);

            // build the feature
            feature.setAttribute("runEnd", new Date());
            feature.setAttribute("itemStatus", "COMPLETED");
            feature.setAttribute("itemStatusMessage",
                    "Change Matrix Process completed successfully");
            feature.setAttribute("layerName", rasterName);
            feature.setAttribute("changeMatrix", JSONSerializer.toJSON(changeMatrix).toString());

            ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
            output.add(feature);

            features = wfsLogProcess.execute(output, typeName, wsName, storeName, filter, false,
                    new NullProgressListener());

            // //////////////////////////////////////////////////////////////////////
            // Return the computed Change Matrix ...
            // //////////////////////////////////////////////////////////////////////
            return changeMatrix;
        } catch (Exception e) {

            if (features != null) {
                // //////////////////////////////////////////////////////////////////////
                // Updating WFS ...
                // //////////////////////////////////////////////////////////////////////
                /**
                 * Update Feature Attributes and LOG into the DB
                 */
                filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

                SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter)
                        .toArray(new SimpleFeature[1])[0]);

                // build the feature
                feature.setAttribute("runEnd", new Date());
                feature.setAttribute("itemStatus", "FAILED");
                feature.setAttribute(
                        "itemStatusMessage",
                        "There was an error while while processing Input parameters: "
                                + e.getMessage());

                ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
                output.add(feature);

                features = wfsLogProcess.execute(output, typeName, wsName, storeName, filter,
                        false, new NullProgressListener());
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
     * Transform the provided {@link Geometry} in world coordinates into
     * 
     * @param roi
     * @param gridToWorld
     * @return
     * @throws Exception
     */
    private static ROI prepareROI(Geometry roi, AffineTransform gridToWorld) throws Exception {
        final Shape cropRoiLS2 = new LiteShape2(roi, ProjectiveTransform.create(gridToWorld)
                .inverse(), null, true, 1);
        return new ROIShape(cropRoiLS2);
    }

    /**
     * Transform the provided {@link Geometry} in world coordinates into
     * 
     * @param roi
     * @param gridToWorld
     * @return
     * @throws Exception
     */
    private static ROI prepareROIGeometry(Geometry roi, AffineTransform gridToWorld)
            throws Exception {

        Geometry projected = JTS.transform(roi, ProjectiveTransform.create(gridToWorld).inverse());

        return new ROIGeometry(projected);
    }

    /**
     * Replace or add the provided parameter in the read parameters
     */
    private <T> GeneralParameterValue[] replaceParameter(GeneralParameterValue[] readParameters,
            Object value, ParameterDescriptor<T> pd) {

        // scan all the params looking for the one we want to add
        for (GeneralParameterValue gpv : readParameters) {
            // in case of match of any alias add a param value to the lot
            if (gpv.getDescriptor().getName().equals(pd.getName())) {
                ((ParameterValue) gpv).setValue(value);
                // leave
                return readParameters;
            }
        }

        // add it to the array
        // add to the list
        GeneralParameterValue[] readParametersClone = new GeneralParameterValue[readParameters.length + 1];
        System.arraycopy(readParameters, 0, readParametersClone, 0, readParameters.length);
        final ParameterValue<T> pv = pd.createValue();
        pv.setValue(value);
        readParametersClone[readParameters.length] = pv;
        readParameters = readParametersClone;
        return readParameters;
    }

    /**
     * @return an hardcoded ChangeMatrixOutput usefull for testing
     */
    private static final ChangeMatrixDTO getTestMap() {

        ChangeMatrixDTO s = new ChangeMatrixDTO();

        s.add(new ChangeMatrixElement(0, 0, 16002481));
        s.add(new ChangeMatrixElement(0, 35, 0));
        s.add(new ChangeMatrixElement(0, 1, 0));
        s.add(new ChangeMatrixElement(0, 36, 4));
        s.add(new ChangeMatrixElement(0, 37, 4));

        s.add(new ChangeMatrixElement(1, 0, 0));
        s.add(new ChangeMatrixElement(1, 35, 0));
        s.add(new ChangeMatrixElement(1, 1, 3192));
        s.add(new ChangeMatrixElement(1, 36, 15));
        s.add(new ChangeMatrixElement(1, 37, 0));

        s.add(new ChangeMatrixElement(35, 0, 0));
        s.add(new ChangeMatrixElement(35, 35, 7546));
        s.add(new ChangeMatrixElement(35, 1, 0));
        s.add(new ChangeMatrixElement(35, 36, 0));
        s.add(new ChangeMatrixElement(35, 37, 16));

        s.add(new ChangeMatrixElement(36, 0, 166));
        s.add(new ChangeMatrixElement(36, 35, 36));
        s.add(new ChangeMatrixElement(36, 1, 117));
        s.add(new ChangeMatrixElement(36, 36, 1273887));
        s.add(new ChangeMatrixElement(36, 37, 11976));

        s.add(new ChangeMatrixElement(37, 0, 274));
        s.add(new ChangeMatrixElement(37, 35, 16));
        s.add(new ChangeMatrixElement(37, 1, 16));
        s.add(new ChangeMatrixElement(37, 36, 28710));
        s.add(new ChangeMatrixElement(37, 37, 346154));

        return s;
    }

    /**
     * @param geomPolys
     * @param geom
     */
    public static void extractPolygons(Collection<Polygon> geomPolys, Geometry geom) {
        if (geom instanceof MultiPolygon) {
            for (int i = 0; i < ((MultiPolygon) geom).getNumGeometries(); i++) {
                Geometry g = ((MultiPolygon) geom).getGeometryN(i);
                if (g instanceof Polygon) {
                    if (g.getGeometryType().compareToIgnoreCase("Polygon") == 0) {
                        g.setSRID(geom.getSRID());
                        geomPolys.add((Polygon) g);
                    }
                }
            }
        } else if (geom instanceof Polygon) {
            if (geom.getGeometryType().compareToIgnoreCase("Polygon") == 0) {
                geomPolys.add((Polygon) geom);
            }
        }
    }

    /**
     * @param retValue
     * @return the number of steps processed, or 0 if none was done, or -1 on error.
     * @throws DataSourceException
     */
    public static int generateOverviews(AbstractGridCoverage2DReader abstractGridCoverage2DReader)
            throws DataSourceException {
        final File geotiffFile = (File) abstractGridCoverage2DReader.getSource();
        // ////
        // Adding Overviews
        // ////

        int tileH = 512;
        int tileW = 512;

        /** computing the number of steps **/
        GridEnvelope gridRange = abstractGridCoverage2DReader.getOriginalGridRange();

        int height = gridRange.getSpan(1);
        int width = gridRange.getSpan(0);

        int ratioH = (int) Math.ceil((1.0 * height) / tileH);
        int ratioW = (int) Math.ceil((1.0 * width) / tileW);

        int nStepsH = 0;
        int nStepsW = 0;

        if (ratioH >= 2) {
            nStepsH = (int) Math.floor(Math.log(ratioH) / Math.log(2));
        }

        if (ratioW >= 2) {
            nStepsW = (int) Math.floor(Math.log(ratioW) / Math.log(2));
        }

        int numSteps = Math.min(nStepsH, nStepsW);
        int downSampleSteps = 2;

        if (numSteps > 0) {
            final OverviewsEmbedder oe = new OverviewsEmbedder();
            oe.setDownsampleStep(downSampleSteps);
            oe.setNumSteps(numSteps);
            oe.setScaleAlgorithm(OverviewsEmbedder.SubsampleAlgorithm.Nearest.toString());
            oe.setTileCache(JAI.getDefaultInstance().getTileCache());
            // oe.setTileHeight(tileH);
            // oe.setTileWidth(tileW);
            oe.setSourcePath(geotiffFile.getAbsolutePath());

            EmbedderListener listener = new EmbedderListener(geotiffFile.getAbsolutePath());
            // add logger/listener
            oe.addProcessingEventListener(listener);

            // run
            oe.run(); // should block until terminated
            return listener.isSuccess() ? numSteps : -1;
        } else
            return 0;
    }

    static class EmbedderListener implements ProcessingEventListener {
        final String filename;

        boolean success = false;

        public EmbedderListener(String filename) {
            this.filename = filename;
        }

        public boolean isSuccess() {
            return success;
        }

        public void exceptionOccurred(ExceptionEvent event) {
            success = false;
        }

        public void getNotification(ProcessingEvent event) {
            if (event.getPercentage() == 100.0) {
                success = true;
            }
        }
    }
}
