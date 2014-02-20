package org.geoserver.wps.gs.soilsealing.model;

public class SoilSealingIndex {

    private int id;
    private String name;
    public SoilSealingIndex(int id, String name) {
        super();
        this.id = id;
        this.name = name;
    }
    /**
     * @return the id
     */
    public int getId() {
        return id;
    }
    /**
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }
    /**
     * @return the name
     */
    public String getName() {
        return name;
    }
    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }
    
    
}
