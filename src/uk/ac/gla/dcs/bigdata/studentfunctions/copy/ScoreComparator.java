package uk.ac.gla.dcs.bigdata.studentfunctions.copy;

import java.io.Serializable;
import java.util.Comparator;

public class ScoreComparator implements Comparator<Double>, Serializable {
	@Override
	public int compare(Double d1, Double d2) {
		return (int) (d1-d2);
	}
		

}
