package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.util.AccumulatorV2;

import uk.ac.gla.dcs.bigdata.studentstructures.RankedResultQuery;

public class RankedResultAccumulator extends AccumulatorV2<RankedResultQuery, List<RankedResultQuery>> {

	private static final long serialVersionUID = 1L;
	private List<RankedResultQuery> rankedresultquery = new ArrayList<>();

	@Override
	public void add(RankedResultQuery v) {
		// TODO Auto-generated method stub
		this.rankedresultquery.add(v);
		
	}

	@Override
	public boolean isZero() {
		// TODO Auto-generated method stub
//		Integer length = this.rankedresultquery.size();
//		if (length>0) {
//			return true;
//		}
		return true;
	}

	@Override
	public void merge(AccumulatorV2<RankedResultQuery, List<RankedResultQuery>> other) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reset() {
		this.rankedresultquery = new ArrayList<>();
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<RankedResultQuery> value() {
		// TODO Auto-generated method stub
		return this.rankedresultquery;
	}



	@Override
	public AccumulatorV2<RankedResultQuery, List<RankedResultQuery>> copy() {
		// TODO Auto-generated method stub
		RankedResultAccumulator newacc = new RankedResultAccumulator();
		return newacc;
	}




}
