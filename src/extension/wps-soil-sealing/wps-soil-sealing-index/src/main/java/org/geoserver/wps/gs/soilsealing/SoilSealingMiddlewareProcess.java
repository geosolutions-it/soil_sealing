package org.geoserver.wps.gs.soilsealing;

import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.gs.soilsealing.SoilSealingAdministrativeUnit.AuSelectionType;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.JTS;
import org.geotools.process.gs.GSProcess;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.metadata.spatial.PixelOrientation;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;

public abstract class SoilSealingMiddlewareProcess implements GSProcess {

    /**
     * Geometry and Filter Factories
     */
    private static final FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
    protected static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(new PrecisionModel());
    
    /**
     * The GeoServer Catalog
     */
    protected Catalog catalog;
    /**
     * The GeoServer Bean
     */
    protected GeoServer geoserver;

    /**
     * Default Constructor
     * 
     * @param catalog
     * @param geoserver
     */
    public SoilSealingMiddlewareProcess(Catalog catalog, GeoServer geoserver) {
        super();
        
        this.catalog = catalog;
        this.geoserver = geoserver;
    }

    /**
     * 
     * 
     * @param nowFilter
     * @param admUnits
     * @param admUnitSelectionType
     * @param ciReference
     * @param geoCodingReference
     * @param populationReference
     * @param municipalities
     * @param rois
     * @param populations
     * @param referenceYear
     * @param currentYear
     * @throws IOException
     * @throws NoSuchAuthorityCodeException
     * @throws FactoryException
     * @throws TransformException
     * @throws NoninvertibleTransformException
     */
    protected void prepareAdminROIs(Filter nowFilter, String admUnits,
            AuSelectionType admUnitSelectionType, CoverageInfo ciReference,
            FeatureTypeInfo geoCodingReference, FeatureTypeInfo populationReference,
            List<String> municipalities, List<Geometry> rois, List<List<Integer>> populations,
            final String referenceYear, final String currentYear) throws IOException,
            NoSuchAuthorityCodeException, FactoryException, TransformException,
            NoninvertibleTransformException {
        // extract administrative units and geometries
        // //
        // GRID TO WORLD preparation from reference
        // //
        final AffineTransform gridToWorldCorner = (AffineTransform) ((GridGeometry2D) ciReference.getGrid()).getGridToCRS2D(PixelOrientation.UPPER_LEFT);
        final CoordinateReferenceSystem referenceCrs = ciReference.getCRS();
        
        for (String au : admUnits.split(",")) {
            SoilSealingAdministrativeUnit sAu = new SoilSealingAdministrativeUnit(au, geoCodingReference, populationReference);
            if (admUnitSelectionType == AuSelectionType.AU_LIST) {
                Geometry roi = null;
                int referencePopulation = 0;
                int currentPopulation = 0;
                switch (sAu.getType()) {
                case MUNICIPALITY :
                    populateInputLists(nowFilter, referenceYear, currentYear,
                            gridToWorldCorner, referenceCrs, rois, populations, sAu);
                    municipalities.add(sAu.getName() + " - " + sAu.getParent());
                    break;
                case DISTRICT:
                    for(SoilSealingAdministrativeUnit ssAu : sAu.getSubs())
                    {
                        if (roi == null) roi = toRasterSpace(ssAu.getTheGeom(), referenceCrs, gridToWorldCorner);
                        else roi = GEOMETRY_FACTORY.buildGeometry(Arrays.asList(roi, toRasterSpace(ssAu.getTheGeom(), referenceCrs, gridToWorldCorner))).union();
                        referencePopulation += ssAu.getPopulation().get(referenceYear);
                        if (nowFilter != null) currentPopulation += ssAu.getPopulation().get(currentYear);
                    }
                    rois.add(roi);
                    populations.get(0).add(referencePopulation);
                    if (nowFilter != null) populations.get(1).add(currentPopulation);
                    municipalities.add(sAu.getName() + " - " + sAu.getParent());
                    break;
                case REGION:
                    for(SoilSealingAdministrativeUnit ssAu : sAu.getSubs())
                    {
                        for(SoilSealingAdministrativeUnit sssAu : ssAu.getSubs()) {
                            if (roi == null) roi = toRasterSpace(sssAu.getTheGeom(), referenceCrs, gridToWorldCorner);
                            else roi = GEOMETRY_FACTORY.buildGeometry(Arrays.asList(roi, toRasterSpace(sssAu.getTheGeom(), referenceCrs, gridToWorldCorner))).union();
                            referencePopulation += sssAu.getPopulation().get(referenceYear);
                            if (nowFilter != null) currentPopulation += sssAu.getPopulation().get(currentYear);
                        }
                    }
                    rois.add(roi);
                    populations.get(0).add(referencePopulation);
                    if (nowFilter != null) populations.get(1).add(currentPopulation);
                    municipalities.add(sAu.getName() + " - " + sAu.getParent());
                    break;
                }
            } else if (admUnitSelectionType == AuSelectionType.AU_SUBS) {
                switch (sAu.getType()) {
                case MUNICIPALITY :
                    populateInputLists(nowFilter, referenceYear, currentYear,
                            gridToWorldCorner, referenceCrs, rois, populations, sAu);
                    municipalities.add(sAu.getName() + " - " + sAu.getParent());
                    break;
                case DISTRICT:
                    for(SoilSealingAdministrativeUnit ssAu : sAu.getSubs())
                    {
                        populateInputLists(nowFilter, referenceYear, currentYear,
                                gridToWorldCorner, referenceCrs, rois, populations, ssAu);
                        municipalities.add(ssAu.getName() + " - " + ssAu.getParent());
                    }
                    break;
                case REGION:
                    for(SoilSealingAdministrativeUnit ssAu : sAu.getSubs())
                    {
                        for(SoilSealingAdministrativeUnit sssAu : ssAu.getSubs()) {
                            populateInputLists(nowFilter, referenceYear, currentYear,
                                    gridToWorldCorner, referenceCrs, rois, populations, sssAu);
                            municipalities.add(sssAu.getName() + " - " + sssAu.getParent());
                        }
                    }
                    break;
                }
            }
        }
    }
    
    /**
     * 
     * @param values
     * @return
     */
    static protected double[][] cleanUpValues(double[][] values) {
        if (values != null) {
            int i = 0;
            for (double[] d : values) {
                if (d != null) {
                    int j = 0;
                    for (double v : d) {
                        v = Double.isNaN(v) || Double.isInfinite(v) ? 0 : v;
                        values[i][j++] = v;
                    }
                }
                i++;
            }
        }
        
        return values;
    }

    /**
     * @param nowFilter
     * @param referenceYear
     * @param currentYear
     * @param gridToWorldCorner
     * @param referenceCrs
     * @param rois
     * @param populations
     * @param sAu
     * @throws NoSuchAuthorityCodeException
     * @throws FactoryException
     * @throws TransformException
     * @throws NoninvertibleTransformException
     */
    protected void populateInputLists(Filter nowFilter, final String referenceYear,
            final String currentYear, final AffineTransform gridToWorldCorner,
            final CoordinateReferenceSystem referenceCrs, List<Geometry> rois,
            List<List<Integer>> populations, SoilSealingAdministrativeUnit sAu)
            throws NoSuchAuthorityCodeException, FactoryException, TransformException,
            NoninvertibleTransformException {
        rois.add(toRasterSpace(sAu.getTheGeom(), referenceCrs, gridToWorldCorner));
        populations.get(0).add(sAu.getPopulation().get(referenceYear));
        if (nowFilter != null) populations.get(1).add(sAu.getPopulation().get(currentYear));
    }

    /**
     * 
     * @param theGeom
     * @param referenceCrs
     * @param gridToWorldCorner
     * @return
     * @throws NoSuchAuthorityCodeException
     * @throws FactoryException
     * @throws MismatchedDimensionException
     * @throws TransformException
     * @throws NoninvertibleTransformException
     */
    protected Geometry toRasterSpace(Geometry theGeom, CoordinateReferenceSystem referenceCrs, AffineTransform gridToWorldCorner) throws NoSuchAuthorityCodeException, FactoryException, MismatchedDimensionException, TransformException, NoninvertibleTransformException {
        // check if we need to reproject the ROI from WGS84 (standard in the input) to the reference CRS
        final CoordinateReferenceSystem targetCrs = CRS.decode("EPSG:"+theGeom.getSRID(), true);
        if (CRS.equalsIgnoreMetadata(referenceCrs, targetCrs)) {
            Geometry rasterSpaceGeometry = JTS.transform(theGeom, new AffineTransform2D(gridToWorldCorner.createInverse()));
            return DouglasPeuckerSimplifier.simplify(rasterSpaceGeometry, 1);
        } else {
            // reproject
            MathTransform transform = CRS.findMathTransform(targetCrs, referenceCrs, true);
            Geometry roiPrj;
            if (transform.isIdentity()) {
                roiPrj = theGeom;
            } else {
                roiPrj = JTS.transform(theGeom, transform);
            }
            return JTS.transform(roiPrj, ProjectiveTransform.create(gridToWorldCorner).inverse());
        }
    }
}