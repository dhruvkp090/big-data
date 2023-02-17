package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class TokenizedNewsArticle {
    List<String> tokenizedTitle;
    int length;
    TokenFrequency frequency;
    NewsArticle article;

    public TokenizedNewsArticle() {
        this.length = 0;
        this.frequency = null;
        this.article = null;
    }

    public TokenizedNewsArticle(List<String> title, int length, TokenFrequency frequency, NewsArticle article) {
        this.tokenizedTitle = title;
        this.length = length;
        this.frequency = frequency;
        this.article = article;
    }

    public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

    public List<String> getTitle() {
        return tokenizedTitle;
    }

    public void setTitle(List<String> title) {
        this.tokenizedTitle = title;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public TokenFrequency getFrequency() {
        return frequency;
    }

    public void setFrequency(TokenFrequency frequency) {
        this.frequency = frequency;
    }
}
