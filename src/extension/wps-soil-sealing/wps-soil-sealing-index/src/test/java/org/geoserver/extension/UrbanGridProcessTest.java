package org.geoserver.extension;

import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.media.jai.TiledImage;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.factory.GeoTools;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.geometry.Envelope2D;
import org.geotools.referencing.CRS;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

public class UrbanGridProcessTest {

    public static final int DEF_H = 256;

    public static final int DEF_W = 256;

    public static final int DEF_TILE_H = 32;

    public static final int DEF_TILE_W = 32;

    private static CoordinateReferenceSystem crs;

    private static GridCoverage2D referenceCoverage;

    private static GridCoverage2D nowCoverage;

    private static ArrayList<Geometry> geomListRaster;

    private static ArrayList<Geometry> geomListUtm32N;

    private static UrbanGridProcess urbanProcess;

    private static String pathToRefShp = "";

    private static String pathToCurShp = "";
    
    
    @BeforeClass
    public static void initialSetup() throws NoSuchAuthorityCodeException, FactoryException, IOException{
        crs = CRS.decode("EPSG:32632");
        
        referenceCoverage = createImage(true);
        nowCoverage = createImage(false);
        
        // Geometries in Raster space for indexes 7a-8-9-10
        geomListRaster = new ArrayList<Geometry>();
        
        GeometryFactory fact = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[5];
        for (int i = 0; i < coordinates.length; i++) {
            if (i == 0 || i == 4) {
                coordinates[i] = new Coordinate(0, 0);
            } else if (i == 3) {
                coordinates[i] = new Coordinate(DEF_W / 2, 0);
            } else if (i == 1) {
                coordinates[i] = new Coordinate(0, DEF_H / 2);
            } else {
                coordinates[i] = new Coordinate(DEF_W / 2, DEF_H / 2);
            }
        }
        LinearRing linear = new GeometryFactory().createLinearRing(coordinates);
        Polygon poly = new Polygon(linear, null, fact);
        
        geomListRaster.add(poly);
        
        // Geometries in UTM 32 N
        geomListUtm32N = new ArrayList<Geometry>();
        
        Geometry geom = (Geometry) poly.clone();
        
        geom.setSRID(32632);
        geomListUtm32N.add(poly);
        
        urbanProcess = new UrbanGridProcess(pathToRefShp, pathToCurShp);
    }
    
    
    
    @Test
    public void test() {
        
    }

    
    
    public static GridCoverage2D createImage(boolean reference){

        SampleModel sm = new ComponentSampleModel(DataBuffer.TYPE_BYTE, DEF_W, DEF_H, 1, DEF_W,
                new int[] { 0 });

        TiledImage img = new TiledImage(sm, DEF_TILE_W, DEF_TILE_H);

        int minX = 0;
        int maxX = 0;
        int minY = 0;
        int maxY = 0;

        if (reference) {
            minX = 10;
            maxX = 20;
            minY = 10;
            maxY = 20;
        } else {
            minX = 11;
            maxX = 20;
            minY = 10;
            maxY = 20;
        }      

        int minPolStart = 0;
        int maxPolStart = 5;
        
        for (int i = minPolStart; i < maxPolStart; i++) {
            for (int j = minPolStart; j < maxPolStart; j++) {
                img.setSample(i, j, 0, 1);
            }
        }
        
        for (int i = minX; i < maxX; i++) {
            for (int j = minY; j < maxY; j++) {
                img.setSample(i, j, 0, 1);
            }
        }
        
        Envelope envelope = new Envelope2D(crs, 0, 0, DEF_W, DEF_H);

        GridCoverage2D result = new GridCoverageFactory(GeoTools.getDefaultHints()).create("test",
                img, envelope);

        return result;
    }
    
}
