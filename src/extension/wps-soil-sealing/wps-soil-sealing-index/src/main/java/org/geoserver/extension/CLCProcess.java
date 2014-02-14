package org.geoserver.extension;

import it.geosolutions.jaiext.bandmerge.BandMergeDescriptor;
import it.geosolutions.jaiext.stats.Statistics;
import it.geosolutions.jaiext.stats.Statistics.StatsType;
import it.geosolutions.jaiext.zonal.ZonalStatsDescriptor;
import it.geosolutions.jaiext.zonal.ZoneGeometry;

import java.awt.image.RenderedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.media.jai.ROI;
import javax.media.jai.RenderedOp;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.factory.GeoTools;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.jaitools.imageutils.ROIGeometry;

import com.vividsolutions.jts.geom.Geometry;

public class CLCProcess implements GSProcess {

    private static final int ZERO_IDX = 0;

    public static final int FIRST_INDEX = 1;

    public static final int SECOND_INDEX = 2;

    public static final int THIRD_INDEX = 3;

    public static final int FOURTH_INDEX = 4;

    public static final double PIXEL_AREA = 400;

    public static final double UPPER_BOUND_INDEX_4 = 1.5d;

    public static final double LOWER_BOUND_INDEX_4 = 0.5d;

    public enum IndexColor {
        GREEN(0), YELLOW(1), RED(2), BLUE(3);

        private final double value;

        IndexColor(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        public static IndexColor valueOf(double value) {
            return IndexColor.values()[(int) value];
        }

    }

    // HP to verify
    // HP1 = geometries in raster space
    // HP2 = Coverages already cropped and transformed to the Raster Space

    @DescribeResult(name = "CLCprocess", description = "CLC indexes", type = List.class)
    public List<StatisticContainer> execute(
            @DescribeParameter(name = "reference", description = "Name of the reference raster") GridCoverage2D referenceCoverage,
            @DescribeParameter(name = "now", description = "Name of the new raster") GridCoverage2D nowCoverage,
            @DescribeParameter(name = "classes", collectionType = Integer.class, min = 1, description = "The domain of the classes used in input rasters") Set<Integer> classes,
            @DescribeParameter(name = "index", min = 1, description = "Index to calculate") int index,
            @DescribeParameter(name = "pixelarea", min = 0, description = "Pixel Area") Double pixelArea,
            @DescribeParameter(name = "rois", min = 1, description = "Administrative Areas") List<Geometry> rois,
            @DescribeParameter(name = "populations", min = 0, description = "Populations for each Area") List<List<Integer>> populations,
            @DescribeParameter(name = "coeff", min = 0, description = "Coefficient used in the 9-10 indexes calculations") Double coeff,
            @DescribeParameter(name = "percent", min = 0, description = "Indicator if the first index must be set in percentual") Boolean multiplier) {

        // First check on the number of input Coverages for the provided index
        boolean refExists = referenceCoverage != null;
        boolean nowExists = nowCoverage != null;

        if (index > FIRST_INDEX && (!nowExists || !refExists)) {
            throw new IllegalArgumentException("This index needs 2 input images");
        } else if (!nowExists && !refExists) {
            throw new IllegalArgumentException("No Coverages provided");
        }

        int numAreas = rois.size();
        int numPop = populations.size();
        if(numPop < 2 && (index == THIRD_INDEX || index == FOURTH_INDEX)){
            throw new IllegalArgumentException("Some Populations are not present");
        }
        int numPopRef = populations.get(ZERO_IDX).size();
        int numPopNow = populations.get(1).size();

        // PixelArea value
        double area = 0;
        if (pixelArea == null) {
            area = PIXEL_AREA;
        } else {
            area = pixelArea;
        }
        
        boolean percentual = false;
        if(multiplier!=null){
            percentual = multiplier;
        }
        // Other check related to the indexes
        switch (index) {
        case FIRST_INDEX:
        case SECOND_INDEX:
            break;
        case THIRD_INDEX:
        case FOURTH_INDEX:
            if (numAreas != numPopRef || numAreas != numPopNow) {
                throw new IllegalArgumentException("Some Areas or Populations are not present");
            }
            break;
        default:
            throw new IllegalArgumentException("Wrong index selected");
        }

        RenderedImage inputImage = null;
        // Merging of the 2 images if they are both present or selection of the single image
        if (refExists) {
            if (nowExists) {
                double destinationNoData = 0d;
                inputImage = BandMergeDescriptor.create(null, destinationNoData,
                        GeoTools.getDefaultHints(), referenceCoverage.getRenderedImage(),
                        nowCoverage.getRenderedImage());
            } else {
                inputImage = referenceCoverage.getRenderedImage();
            }
        } else {
            inputImage = nowCoverage.getRenderedImage();
        }

        // Countercheck (Should never get here)
        if (inputImage == null) {
            throw new IllegalArgumentException("The image to calculate does not exists");
        }

        // Statistic object to calculate
        StatsType[] stats = new StatsType[] { StatsType.HISTOGRAM };

        // Further controls on the image band number and initialization of the statistics parameters
        int[] bands;
        double[] minBound;
        double[] maxBound;
        int[] numBins;
        // Band number of the input image
        int numBands = inputImage.getSampleModel().getNumBands();
        // Check if the band number is equal to 2
        boolean multiBanded = numBands == 2;
        if (multiBanded) {
            bands = new int[] { 0, 1 };
            minBound = new double[] { 0, 0 };
            maxBound = new double[] { 255, 255 };
            numBins = new int[] { 255, 255 };
        } else {
            bands = new int[] { 0 };
            minBound = new double[] { 0 };
            maxBound = new double[] { 255 };
            numBins = new int[] { 255 };
        }

        // Creation of a list of ROIs, each one for each Geometry object
        List<ROI> roilist = new ArrayList<ROI>();

        for (Geometry geom : rois) {
            roilist.add(new ROIGeometry(geom));
        }
        // Selection of the parameters
        RenderedOp zonalStats = ZonalStatsDescriptor.create(inputImage, null, null, roilist, null,
                null, false, bands, stats, minBound, maxBound, numBins, null, false, null);

        // Calculation of the results
        List<ZoneGeometry> results = (List<ZoneGeometry>) zonalStats
                .getProperty(ZonalStatsDescriptor.ZS_PROPERTY);

        // Class number
        int numClass = classes.size();

        // Zones counter
        int countZones = 0;

        // Result container
        List<StatisticContainer> container = new ArrayList<StatisticContainer>(numAreas);

        // Selection of the statistics for each Zone
        switch (index) {
        case FIRST_INDEX:
            // Elaboration for a 2-band image
            if (multiBanded) {
                for (ZoneGeometry zone : results) {
                    double[][] coeffCop = calculateCoeffCop(classes, bands, zone, area, percentual);

                    Geometry geo = ((ROIGeometry) zone.getROI()).getAsGeometry();
                    
                    if(percentual){
                        double[] coeffVariation = calculateVariation(numClass, coeffCop);
                        
                        container.add(new StatisticContainer(geo, coeffVariation, null));
                    }else{
                        container.add(new StatisticContainer(geo, coeffCop[ZERO_IDX], coeffCop[1]));
                    }
                }
            } else {
                for (ZoneGeometry zone : results) {
                    double[] coeffCop = calculateCoeffCop(classes, bands, zone, area, percentual)[ZERO_IDX];

                    Geometry geo = ((ROIGeometry) zone.getROI()).getAsGeometry();

                    container.add(new StatisticContainer(geo, coeffCop, null));
                }
            }
            break;
        case SECOND_INDEX:
            for (ZoneGeometry zone : results) {
                double[][] coeffCop = new double[2][numClass];
                for (int b : bands) {

                    Statistics out = zone.getStatsPerBandNoClassifierNoRange(b)[ZERO_IDX];

                    double[] histogram = (double[]) out.getResult();
                    int count = 0;
                    for (Integer clc : classes) {
                        double clcArea = histogram[clc] * area;
                        coeffCop[b][count++] = clcArea;
                    }
                }

                double[] coeffVariation = calculateVariation(numClass, coeffCop);

                Geometry geo = ((ROIGeometry) zone.getROI()).getAsGeometry();

                container.add(new StatisticContainer(geo, coeffVariation, null));
            }
            break;
        case THIRD_INDEX:
            for (ZoneGeometry zone : results) {
                double[] consMarg = calculateCLCSum(classes, bands, zone, area);

                double first = consMarg[ZERO_IDX];
                double second = consMarg[1];
                double areaVar = (second - first);

                double firstPop = populations.get(ZERO_IDX).get(countZones);
                double secondPop = populations.get(1).get(countZones);
                double popVar = (secondPop - firstPop);

                Geometry geo = ((ROIGeometry) zone.getROI()).getAsGeometry();

                double result = 0;

                if(coeff != null){
                    result = (areaVar/popVar)*coeff;
                }else{
                    // Index result
                    if (areaVar >= 0) {
                        if (popVar >= 0) {
                            result = IndexColor.GREEN.getValue();
                        } else {
                            result = IndexColor.YELLOW.getValue();
                        }
                    } else {
                        if (popVar >= 0) {
                            result = IndexColor.RED.getValue();
                        } else {
                            result = IndexColor.BLUE.getValue();
                        }
                    }
                }

                container.add(new StatisticContainer(geo, new double[] { result }, null));
                // Update of the Zones
                countZones++;
            }
            break;
        case FOURTH_INDEX:
            for (ZoneGeometry zone : results) {
                double[] sumArray = calculateCLCSum(classes, bands, zone, area);

                double first = sumArray[ZERO_IDX];
                double second = sumArray[1];
                double areaTa = ((second - first) / first);

                double firstPop = populations.get(ZERO_IDX).get(countZones);
                double secondPop = populations.get(1).get(countZones);
                double popTa = ((secondPop - firstPop) / firstPop);

                Geometry geo = ((ROIGeometry) zone.getROI()).getAsGeometry();

                double sprawl = areaTa / popTa;

                double result = 0;

                if (sprawl > UPPER_BOUND_INDEX_4) {
                    result = IndexColor.RED.getValue();
                } else if (sprawl < LOWER_BOUND_INDEX_4) {
                    result = IndexColor.GREEN.getValue();
                } else {
                    result = IndexColor.YELLOW.getValue();
                }

                container.add(new StatisticContainer(geo, new double[] { result }, null));

                // Update of the Zones
                countZones++;
            }
            break;
        }
        return container;
    }

    /**
     * @param classes
     * @param bands
     * @param zone
     * @param consMarg
     */
    private double[] calculateCLCSum(Set<Integer> classes, int[] bands, ZoneGeometry zone,
            double area) {

        double[] consMarg = new double[bands.length];

        for (int b : bands) {

            Statistics out = zone.getStatsPerBandNoClassifierNoRange(b)[ZERO_IDX];

            double[] histogram = (double[]) out.getResult();
            for (Integer clc : classes) {
                double clcArea = histogram[clc] * area;
                consMarg[b] += clcArea;
            }
        }

        return consMarg;
    }

    /**
     * @param numClass
     * @param coeffCop
     * @param coeffVariation
     */
    private double[] calculateVariation(int numClass, double[][] coeffCop) {

        double[] coeffVariation = new double[numClass];

        for (int i = 0; i < numClass; i++) {
            double first = coeffCop[0][i];
            double second = coeffCop[1][i];
            coeffVariation[i] = ((second - first) / first) * 100;
        }

        return coeffVariation;
    }

    private double[][] calculateCoeffCop(Set<Integer> classes, int[] bands, ZoneGeometry zone,
            double area, boolean percentual) {

        double[][] coeffCop = new double[bands.length][classes.size()];

        double multiplier = 1;
        
        if(percentual){
            multiplier = 100;
        }
        
        double adminArea;
        for (int b : bands) {

            Statistics out = zone.getStatsPerBandNoClassifierNoRange(b)[ZERO_IDX];

            adminArea = out.getNumSamples() * area;

            double[] histogram = (double[]) out.getResult();
            int count = 0;
            for (Integer clc : classes) {
                double clcArea = histogram[clc] * area;
                coeffCop[b][count++] = clcArea / adminArea * multiplier;
            }
        }

        return coeffCop;
    }

    public static class StatisticContainer {

        private Geometry geom;

        private double[] resultsRef;

        private double[] resultsNow;

        public StatisticContainer() {
        }

        public StatisticContainer(Geometry geom, double[] resultsRef, double[] resultsNow) {
            this.geom = geom;
            this.resultsRef = resultsRef;
            this.resultsNow = resultsNow;
        }

        public Geometry getGeom() {
            return geom;
        }

        public void setGeom(Geometry geom) {
            this.geom = geom;
        }

        public double[] getResults() {
            return resultsRef;
        }

        public double[] getResultsRef() {
            return resultsRef;
        }

        public void setResultsRef(double[] resultsRef) {
            this.resultsRef = resultsRef;
        }

        public double[] getResultsNow() {
            return resultsNow;
        }

        public void setResultsNow(double[] resultsNow) {
            this.resultsNow = resultsNow;
        }

    }

}
