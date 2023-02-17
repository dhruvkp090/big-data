package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

//This is the class representing the information about the corpus
public class CorpusSummary implements Serializable {
	private static final long serialVersionUID = 1L;
	long totalDocuments; //the total number of documents in the corpus
	float averageDocumentLength; //the average length of the corpus
	TokenFrequency queryTermsFrequency; //the sum of term frequencies for the set of all the terms in queries across all documents
	
	public CorpusSummary(int totalDocuments, float averageDocumentLength,TokenFrequency queryTermsFrequency) {
		this.totalDocuments = totalDocuments;
		this.averageDocumentLength = averageDocumentLength;
		this.queryTermsFrequency = queryTermsFrequency;
		
	}
	
	public long getTotalDocuments(){
		return totalDocuments;
	}
	
	public float getAverageDocumentLength(){
		return averageDocumentLength;
	}
	
	public TokenFrequency getQueryTermsFrequency(){
		return queryTermsFrequency;
	}
	
	public void setTotalDocuments(int totalDocuments){
		this.totalDocuments = totalDocuments;
	}
	
	public void setAverageDocumentLength(float averageDocumentLength){
		this.averageDocumentLength = averageDocumentLength;
	}
	
	public void setQueryTermsFrequency(TokenFrequency queryTermsFrequency){
		this.queryTermsFrequency = queryTermsFrequency;
	}
	
}
