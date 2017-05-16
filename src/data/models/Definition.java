package data.models;

public class Definition {
    private String mContent;
    private String mTranslation;

    public Definition(){

    }

    public Definition(String content, String translation){
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
