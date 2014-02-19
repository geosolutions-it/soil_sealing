/* Copyright (c) 2001 - 2014 OpenPlans - www.openplans.org. All rights 
 * reserved. This code is licensed under the GPL 2.0 license, available at the 
 * root application directory.
 */
package org.geoserver.wps.gs.soilsealing;

/**
 * This Bean represent a single element of a ChangeMatrix
 * @author DamianoG, GeoSolutions 
 *
 */
public class ChangeMatrixElement implements Comparable<ChangeMatrixElement>{

    /**
     * The Pixel class type, Image at time 0
     */
    private int ref;
    /**
     * The Pixel class type, Image at time 1 
     */
    private int now;
    /**
     * The number of pixel that has been changed from class ref to class now
     */
    private long pixels;
    
    /**
     * @param ref
     * @param now
     * @param pixels
     */
    public ChangeMatrixElement(int ref, int now, long pixels) {
        this.ref = ref;
        this.now = now;
        this.pixels = pixels;
    }

    /**
     * @return the ref
     */
    public int getRef() {
        return ref;
    }

    /**
     * @return the now
     */
    public int getNow() {
        return now;
    }

    /**
     * @return the pixels
     */
    public long getPixels() {
        return pixels;
    }

    /**
     * @param ref the ref to set
     */
    public void setRef(int ref) {
        this.ref = ref;
    }

    /**
     * @param now the now to set
     */
    public void setNow(int now) {
        this.now = now;
    }

    /**
     * @param pixels the pixels to set
     */
    public void setPixels(int pixels) {
        this.pixels = pixels;
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(ChangeMatrixElement o) {
        if(o.getRef()==this.getRef()&&o.getNow()==this.getNow()&&o.getPixels()==this.getPixels()){
            return 0;
        }
        return 1;
    }    
}
