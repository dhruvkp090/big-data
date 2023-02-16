package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

//This is the class representing the token frequency pairs
public class TokenFrequency<T> implements Serializable{
	private static final long serialVersionUID = 27L;
	Map<String, T> frequency;
	
    public TokenFrequency() {
        this.frequency = null;
    }
	
    public TokenFrequency(HashMap<String, T> frequency) {
        this.frequency = frequency;
    }
    
    public Map<String, T> getFrequency() {
        return frequency;
    }

    public void setFrequency(Map<String, T> frequency) {
        this.frequency = frequency;
    }
    
}
