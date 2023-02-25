package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

/**
 * Represents the information about the Corpus
 * 
 * @param totalDocuments        the number of documents in the corpus
 * @param averageDocumentLength the average document length
 * @param queryTermsFrequency   TokenFrequency object for the entire corpus
 */

public class CorpusSummary implements Serializable {
	private static final long serialVersionUID = 1L;
	long totalDocuments; 
	float averageDocumentLength; 
	TokenFrequency queryTermsFrequency; 

	public CorpusSummary(long totalDocuments, float averageDocumentLength, TokenFrequency queryTermsFrequency) {
		this.totalDocuments = totalDocuments;
		this.averageDocumentLength = averageDocumentLength;
		this.queryTermsFrequency = queryTermsFrequency;

	}

	public long getTotalDocuments() {
		return totalDocuments;
	}

	public float getAverageDocumentLength() {
		return averageDocumentLength;
	}

	public TokenFrequency getQueryTermsFrequency() {
		return queryTermsFrequency;
	}

	public void setTotalDocuments(int totalDocuments) {
		this.totalDocuments = totalDocuments;
	}

	public void setAverageDocumentLength(float averageDocumentLength) {
		this.averageDocumentLength = averageDocumentLength;
	}

	public void setQueryTermsFrequency(TokenFrequency queryTermsFrequency) {
		this.queryTermsFrequency = queryTermsFrequency;
	}

}
