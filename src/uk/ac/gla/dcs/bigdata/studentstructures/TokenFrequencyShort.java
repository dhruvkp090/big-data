package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TokenFrequencyShort implements Serializable {
	private static final long serialVersionUID = 27L;
	Map<String, Short> frequency;
	
    public TokenFrequencyShort() {
        this.frequency = null;
    }
	
    public TokenFrequencyShort(HashMap<String, Short> frequency) {
        this.frequency = frequency;
    }
    
    public Map<String, Short> getFrequency() {
        return frequency;
    }

    public void setFrequency(Map<String, Short> frequency) {
        this.frequency = frequency;
    }
}
