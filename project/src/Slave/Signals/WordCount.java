package project.src.Slave.Signals;

import java.io.Serializable;

public class WordCount implements Serializable{

    private final int count;
    private final String word;

    public WordCount(String word, int count){
        this.word = word;
        this.count = count;
    }

    public WordCount(String word){
        this.count = 1;
        this.word = word;
    }

    public String getWord(){
        return this.word;
    }

    public int getCount(){
        return this.count;
    }
}