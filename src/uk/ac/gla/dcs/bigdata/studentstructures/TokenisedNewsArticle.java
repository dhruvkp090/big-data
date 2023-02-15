package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.HashMap;
import java.util.List;

public class TokenisedNewsArticle {
    String id;
    List<String> title;
    int length;
    HashMap<String, Integer> frequency;

    public TokenisedNewsArticle(String id, List<String> title, int length, HashMap<String, Integer> frequency) {
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

    public HashMap<String, Integer> getFrequency() {
        return frequency;
    }

    public void setFrequency(HashMap<String, Integer> frequency) {
        this.frequency = frequency;
    }
}
