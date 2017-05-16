package data.models;

public class DefinitionEntry{
    private long mContentId;
    private long mTranslationId;

    public DefinitionEntry(long contentId, long translationId){
        mContentId = contentId;
        mTranslationId = translationId;
    }

    public long getContentId(){return mContentId;}
    public long getTranslationId(){return mTranslationId;}

    public void setContentId(long contentId){mContentId = contentId;}
    public void setTranslationId(long translationId){mTranslationId = translationId;}
}