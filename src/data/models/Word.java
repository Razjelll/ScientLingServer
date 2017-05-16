package data.models;

import java.util.List;

public class Word {

    private String mContent;
    private List<String> mTranslations;
    private Definition mDefinition;
    private List<Sentence> mSentences;
    private List<String> mHints;
    private long mPartOfSpeech;
    private long mCategory;
    private int mDifficulty;
    private String mImage;
    private String mRecord;
    private long mLesson;

    public String getContent(){return mContent;}
    public void setContent(String content) { mContent = content;}

    public List<String> getTranslations(){return mTranslations;}
    public String getTranslation(int position){return mTranslations.get(position);}
    public void setTranslations(List<String> translations){mTranslations = translations;}

    public Definition getDefinition(){return mDefinition;}
    public void setDefinition(Definition definition) {mDefinition =definition;}
    public void setDefinition(String content, String translation){
        mDefinition = new Definition(content,translation);
    }

    public List<Sentence> getSentences(){return mSentences;}
    public Sentence getSentence(int position){return mSentences.get(position);}
    public void setSentences(List<Sentence> sentences) {mSentences = sentences;}

    public List<String> getHints(){return mHints;}
    public String getHint(int position){return mHints.get(position);}
    public void setHints(List<String> hints) {mHints = hints;}

    public long getPartOfSpeech(){return  mPartOfSpeech;}
    public void setPartOfSpeech(long partOfSpeech){mPartOfSpeech = partOfSpeech;}

    public long getCategory(){return mCategory;}
    public void setCategory(long category) {mCategory = category;}

    public int getDifficulty(){return mDifficulty;}
    public void setDifficulty(int difficulty){mDifficulty = difficulty;}

    public String getImage(){return mImage;}
    public void setImage(String image){mImage = image;}

    public String getRecord(){return mRecord;}
    public void setRecord(String record){mRecord = record;}

    public long getLesson(){return mLesson;}
    public void setLesson(long lesson){mLesson = lesson;}

    @Override
    public String toString(){
        StringBuilder stringBuilder = new StringBuilder();
        String translations = null;
        if(mTranslations != null){
            mTranslations.forEach(stringBuilder::append);
            translations = stringBuilder.toString();
            stringBuilder.setLength(0);
        }
        String sentences = null;
        if(mSentences != null){
            for(Sentence s: mSentences){
                stringBuilder.append(s.toString());
            }
            sentences = stringBuilder.toString();
            stringBuilder.setLength(0);
        }

        String hints = null;
        if(mHints != null){
            mHints.forEach(stringBuilder::append);
            hints = stringBuilder.toString();
            stringBuilder.setLength(0);
        }

        return new StringBuilder().append("content: ").append(mContent).append("\n")
                .append("translations: ").append(translations).append("\n")
                .append("definition: ").append(mDefinition!=null?mDefinition.toString():"").append("\n")
                .append("sentences: ").append(sentences).append("\n")
                .append("hints: ").append(hints).append("\n")
                .append("part: ").append(mPartOfSpeech).append("\n")
                .append("category: ").append(mCategory).append("\n")
                .append("difficulty: ").append(mDifficulty).append("\n")
                .append("image: ").append(mImage).append("\n")
                .append("record: ").append(mRecord).append("\n")
                .append("lesson: ").append(mLesson).toString();
    }

}
