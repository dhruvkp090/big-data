package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a map object of tokens and their frequenicies
 * 
 * @param frequency  the map of tokens and frequencies
 */

public class TokenFrequency implements Serializable{
	private static final long serialVersionUID = 27L;
	Map<String, Integer> frequency;
	
    public TokenFrequency() {
        this.frequency = null;
    }
	
    public TokenFrequency(Map<String, Integer> frequency) {
        this.frequency = frequency;
    }
    
    public Map<String, Integer> getFrequency() {
        return frequency;
    }

    public void setFrequency(Map<String, Integer> frequency) {
        this.frequency = frequency;
    }
    
}
