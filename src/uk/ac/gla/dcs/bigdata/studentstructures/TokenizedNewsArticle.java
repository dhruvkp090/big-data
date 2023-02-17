package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.List;

public class TokenizedNewsArticle {
    String id;
    List<String> title;
    int length;
    TokenFrequency frequency;

    public TokenizedNewsArticle() {
        this.id = null;
        this.title = null;
        this.length = 0;
        this.frequency = null;
    }

    public TokenizedNewsArticle(String id, List<String> title, int length, TokenFrequency frequency) {
        this.id = id;
        this.title = title;
        this.length = length;
        this.frequency = frequency;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getTitle() {
        return title;
    }

    public void setTitle(List<String> title) {
        this.title = title;
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
