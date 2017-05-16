package data.models;

import java.util.concurrent.TransferQueue;

/**
 * Created by Razjelll on 29.04.2017.
 */
public class Sentence {
    private String mContent;
    private String mTranslation;

    public Sentence(){

    }

    public Sentence(String content, String translation){
        mContent = content;
        mTranslation = translation;
    }

    public String getContent(){return mContent;}
    public void setContent(String content){mContent = content;}

    public String getTranslation(){return mTranslation;}
    public void setTranslation(String translation) {mTranslation =translation;}

    @Override
    public String toString(){
        return new StringBuilder("content: ").append(mContent).append("\n")
                .append("translation: ").append(mTranslation).append("\n").toString();
    }
}
