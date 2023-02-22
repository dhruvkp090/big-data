package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class RankedResultQuery implements Serializable, Comparable<RankedResultQuery>{

	private static final long serialVersionUID = -2905684103776472843L;
	
	String docid;
	NewsArticle article;
	double score;
	Query query;
	
	public RankedResultQuery() {}
	
	public RankedResultQuery(String docid, NewsArticle article, double score, Query query) {
		super();
		this.docid = docid;
		this.article = article;
		this.score = score;
		this.query = query;
	}

	public String getDocid() {
		return docid;
	}

	public void setDocid(String docid) {
		this.docid = docid;
	}

	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	@Override
	public int compareTo(RankedResultQuery o) {
		return new Double(score).compareTo(o.score);
	}
	
}
