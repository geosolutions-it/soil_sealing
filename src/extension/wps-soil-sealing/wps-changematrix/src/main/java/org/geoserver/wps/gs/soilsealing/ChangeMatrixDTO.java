/* Copyright (c) 2001 - 2014 OpenPlans - www.openplans.org. All rights 
 * reserved. This code is licensed under the GPL 2.0 license, available at the 
 * root application directory.
 */
package org.geoserver.wps.gs.soilsealing;

import it.geosolutions.jaiext.changematrix.ChangeMatrixDescriptor.ChangeMatrix;

import java.util.Set;
import java.util.TreeSet;

/**
 * This is the changeMatrix
 * 
 * @author Damiano Giampaoli, GeoSolutions SAS
 * 
 */
public class ChangeMatrixDTO {

    private String rasterName = null;

    /**
     * The implementation of the changeMatrix as a Set
     */
    private final Set<ChangeMatrixElement> changeMatrix = new TreeSet<ChangeMatrixElement>();

    /**
     * Init the changeMatrix as an empty TreeSet
     * 
     * @param classes
     * @param cm
     */
    public ChangeMatrixDTO(ChangeMatrix cm, Set<Integer> classes, String rasterName, Double multiplier) {
        
        double value = 1;
        
        if(multiplier!=null){
            value = multiplier;
        }
        
        this.setRasterName(rasterName);

        for (Integer elRef : classes) {
            for (Integer elNow : classes) {
                final ChangeMatrixElement cme = new ChangeMatrixElement(elRef, elNow,
                        cm.retrievePairOccurrences(elRef, elNow)*value);
                add(cme);
            }
        }
    }
    
    /**
     * Init the changeMatrix as an empty TreeSet
     * 
     * @param classes
     * @param cm
     */
    public ChangeMatrixDTO(ChangeMatrix cm, Set<Integer> classes, String rasterName) {
        this(cm,classes,rasterName,null);
    }

    /**
     * Default constructor.
     * 
     */
    public ChangeMatrixDTO() {
    }

    /**
     * Add an element to the changeMatrix
     * 
     * @param el
     */
    public void add(ChangeMatrixElement el) {
        this.changeMatrix.add(el);
    }

    /**
     * 
     * @return
     */
    public Set<ChangeMatrixElement> getChangeMatrix() {
        return changeMatrix;
    }

    /**
     * @param rasterName the rasterName to set
     */
    public void setRasterName(String rasterName) {
        this.rasterName = rasterName;
    }

    /**
     * @return the rasterName
     */
    public String getRasterName() {
        return rasterName;
    }

}
