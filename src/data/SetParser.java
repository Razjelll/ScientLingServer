package data;

//import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import data.models.*;
import data.queryExecutors.InsertSetManager;


import javax.naming.NamingException;
        import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class SetParser {
    private final int FAILED = -1;

    private InputStream mInputStream;
    private int mWordsCount;
    private InsertSetManager mInsertManager;

    private boolean mInsertFailed;

    private long mSetId;
    private long mUserId;
    /**Mapa przechowujaca numery id lekcji z aplikacji i odpowiadające im numery lekcji z bazy danych na serwerze*/
    private Map<Long, Long> mLessonMap;

    public SetParser(InputStream inputStream) {
        mInputStream = inputStream;
        mLessonMap = new HashMap<>();
    }

    public void setUderId(long userId){
        mUserId = userId;
    }

    public class ProcessResult{
        private long mId;
        private long mSize;

        public ProcessResult(long id, long size){
            mId = id;
            mSize = size;
        }
        public long getId(){
            return mId;
        }
        public long getSize(){
            return mSize;
        }
        public void setId(long setId){
            mId = setId;
        }
        public void setSize(long size){
            mSize = size;
        }
    }

    public long process() throws IOException, NamingException, ClassNotFoundException, SQLException {
        mInsertManager = new InsertSetManager();
        /*JsonFactory factory = new JsonFactory();
        JsonParser parser = factory.createParser(mInputStream);*/
        JsonParser parser = new data.JsonParser(mInputStream);
        JsonToken token;
        String fieldName;
        while(!parser.isClosed() && (token = parser.nextToken()) != null){
            if(mInsertFailed){
                return -1;
            }
            if(token.equals(JsonToken.FIELD_NAME)){
                fieldName = parser.getCurrentName();
                parser.nextToken(); //ominięcie nawiasu {
                processObject(fieldName, parser);
            }
        }
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("Za pętlą");
        try{
            mInsertManager.updateWordsCount(mSetId, parser.getLength());
        } catch (SQLException e){
            mInsertManager.endFailedTransaction();
            mInsertFailed = true;
        }

        if(!mInsertFailed){
            logger.severe("Transakcja zaakceptowana");
            mInsertManager.endSuccessfulTransaction();
            ///return new ProcessResult(mSetId, );
            return mSetId;
        } else {
            logger.severe("Transakcja padła");
            mInsertManager.endFailedTransaction();
            return -1;
        }
    }

    private final String SET = "set";
    private final String LESSONS = "lessons";
    private final String WORDS = "words";

    private void processObject(String objectName, JsonParser parser) throws IOException, SQLException {
        switch (objectName){
            case SET:
                processSetObject(parser); break;
            case LESSONS:
                processLessonsObject(parser); break;
            case WORDS:
                processWordsObject(parser); break;
        }
    }

    private final String SET_ID = "id";
    private final String SET_NAME = "name";
    private final String SET_L1 = "l1";
    private final String SET_L2 = "l2";
    private final String SET_DESCRIPTION = "desc";

    private void processSetObject(JsonParser parser) throws IOException, SQLException {
        Logger logger = Logger.getLogger(getClass().getName());
        Set set = new Set();
        set.setAuthorId(mUserId);
        while (parser.nextToken() != JsonToken.END_OBJECT){
            String fieldName = parser.getCurrentName();
            switch (fieldName){
                case SET_ID:
                    set.setId(parser.nextLongValue(-1)); break;
                case SET_NAME:
                    set.setName(parser.nextTextValue()); break;
                case SET_L1:
                    set.setL1(parser.nextLongValue(-1)); break;
                case SET_L2:
                    set.setL2(parser.nextLongValue(-1)); break;
                case SET_DESCRIPTION:
                    set.setDescription(parser.nextTextValue()); break;
            }
            if(set.getDescription()==null){
                set.setDescription("");
            }
        }
        mInsertManager.beginTransaction();
        try {
            logger.severe(set.toString());
            mSetId = mInsertManager.insertSet(set);
        } catch (SQLException e) {
            mInsertFailed = true;
            mInsertManager.endFailedTransaction();
        } catch (ClassNotFoundException e) {
            mInsertFailed = true;
            mInsertManager.endFailedTransaction();
        } catch (NamingException e) {
            mInsertFailed = true;
            mInsertManager.endFailedTransaction();
        }
    }

    private final String LESSON_ID = "id";
    private final String LESSON_NAME = "name";
    private final String LESSON_NUMBER = "number";

    private void processLessonsObject(JsonParser parser) throws IOException, SQLException {
        Lesson lesson = null;
        Logger logger = Logger.getLogger(getClass().getName());
        JsonToken token;
        while((token = parser.nextToken()) != JsonToken.END_ARRAY){
            if(token == JsonToken.START_OBJECT){
                lesson = new Lesson();
                parser.nextToken();
            }
            if(token != JsonToken.END_OBJECT){
                String fieldName = parser.getCurrentName();
                switch (fieldName){
                    case LESSON_ID:
                        lesson.setId(parser.nextLongValue(-1)); break;
                    case LESSON_NAME:
                        lesson.setName(parser.nextTextValue()); break;
                    case LESSON_NUMBER:
                        lesson.setNumber(parser.nextIntValue(-1)); break;
                }
            } else {
                logger.severe(lesson.toString());
                lesson.setSetId(mSetId); //TODO tutaj można będzie coś pomyśleć co zrobić jeśli nie nie będize ustawionego zestawu
                long lessonId = 0;
                try {
                    lessonId = mInsertManager.insertLesson(lesson);
                    mLessonMap.put(lesson.getId(), lessonId);
                } catch (SQLException e) {
                    logger.severe(e.toString());
                    mInsertFailed = true;
                    mInsertManager.endFailedTransaction();
                }
            }
        }
    }

    private final String WORD_CONTENT = "content";
    private final String WORD_TRANSLATIONS = "translations";
    private final String WORD_PART = "part";
    private final String WORD_CATEGORY = "category";
    private final String WORD_DIFFICULTY = "difficulty";
    private final String WORD_SENTENCES = "sentences";
    private final String WORD_DEFINITION = "definition";
    private final String WORD_HINTS = "hints";
    private final String WORD_IMAGE ="image";
    private final String WORD_RECORD = "record";
    private final String WORD_LESSON = "lesson";

    private final String TRANSLATION_CONTENT = "c";
    private final String SENTENCE_CONTENT = "c";
    private final String SENTENCE_TRANSLATION = "t";
    private final String HINT_CONTENT = "c";
    private final String DEFINITION_CONTENT = "c";
    private final String DEFINITION_TRANSLATION = "t";

    private void processWordsObject(JsonParser parser) throws IOException, SQLException {
        Logger logger = Logger.getLogger(getClass().getName());
        JsonToken token;
        Word word = null;
        String fieldName;
        while((token = parser.nextToken())!=JsonToken.END_ARRAY){
            if(token == JsonToken.START_OBJECT){
                word = new Word();
                token = parser.nextToken();
                mWordsCount++;
            }
            if(token != JsonToken.END_OBJECT){
                fieldName = parser.getCurrentName();
                switch (fieldName){
                    case WORD_CONTENT:
                        word.setContent(parser.nextTextValue()); break;
                    case WORD_TRANSLATIONS:
                        word.setTranslations(getTranslation(parser)); break;
                    case WORD_DEFINITION:
                        word.setDefinition(getDefinition(parser)); break;
                    case WORD_SENTENCES:
                        word.setSentences(getSentences(parser)); break;
                    case WORD_HINTS:
                        word.setHints(getHints(parser)); break;
                    case WORD_PART:
                        word.setPartOfSpeech(parser.nextLongValue(-1)); break;
                    case WORD_CATEGORY:
                        word.setCategory(parser.nextLongValue(-1)); break;
                    case WORD_DIFFICULTY:
                        word.setDifficulty(parser.nextIntValue(-1)); break;
                    case WORD_IMAGE:
                        word.setImage(parser.nextTextValue()); break;
                    case WORD_RECORD:
                        word.setRecord(parser.nextTextValue()); break;
                    case WORD_LESSON:
                        long lessonId = parser.nextLongValue(-1);
                        word.setLesson(mLessonMap.get(lessonId)); break;
                }
            } else {
                logger.severe(word.toString());
                try {
                    mInsertManager.insertWord(word);
                } catch (ClassNotFoundException e) {
                    mInsertManager.endFailedTransaction();
                } catch (SQLException e) {
                    mInsertManager.endFailedTransaction();
                } catch (NamingException e) {
                    mInsertManager.endFailedTransaction();
                }

            }
        }
    }

    private List<String> getTranslation(JsonParser parser) throws IOException {
        List<String> translationsList = new ArrayList<>();
        String translation;
        JsonToken token;
        while((token = parser.nextToken()) != JsonToken.END_ARRAY){
            if(token==JsonToken.START_OBJECT){
                token = parser.nextToken();
            }
            if(token != JsonToken.END_OBJECT){
                if(parser.getCurrentName().equals(TRANSLATION_CONTENT)){
                    translation = parser.nextTextValue();
                    if(translation != null){
                        translationsList.add(translation);
                    }
                }
            }
        }
        if(translationsList.isEmpty()){
            return null;
        }
        return translationsList;
    }

    private Definition getDefinition(JsonParser parser) throws IOException {
        JsonToken token;
        String fieldName;
        String content = null;
        String translation = null;
        while((token = parser.nextToken())!= JsonToken.END_OBJECT){
            if(token == JsonToken.START_OBJECT){
                parser.nextToken();
                fieldName = parser.getCurrentName();
                switch (fieldName){
                    case DEFINITION_CONTENT:
                        content = parser.nextTextValue(); break;
                    case DEFINITION_TRANSLATION:
                        translation = parser.nextTextValue(); break;
                }
            }
        }
        if(content != null){
            return new Definition(content, translation);
        }
        return null;
    }

    private List<Sentence> getSentences(JsonParser parser) throws IOException {
        JsonToken token;
        String fieldName;
        List<Sentence> sentencesList = new ArrayList<>();
        Sentence sentence = null;
        while((token = parser.nextToken()) != JsonToken.END_ARRAY){
            if(token == JsonToken.START_OBJECT){
                sentence = new Sentence();
                token = parser.nextToken();
            }
            if(token != JsonToken.END_OBJECT){
                fieldName = parser.getCurrentName();
                switch (fieldName){
                    case SENTENCE_CONTENT:
                        sentence.setContent(parser.nextTextValue()); break;
                    case SENTENCE_TRANSLATION:
                        sentence.setTranslation(parser.nextTextValue()); break;
                }
            } else {
                sentencesList.add(sentence);
            }
        }
        if(sentencesList.isEmpty()){
            return null;
        }
        return sentencesList;
    }

    private List<String> getHints(JsonParser parser) throws IOException {
        String fileName;
        List<String> hintsList = new ArrayList<>();
        JsonToken token;
        while((token = parser.nextToken()) != JsonToken.END_ARRAY){
            if(token == JsonToken.START_OBJECT) {
                token = parser.nextToken();
            }
            if(token != JsonToken.END_OBJECT){
                if(parser.getCurrentName().equals(HINT_CONTENT)){
                    String hint = parser.nextTextValue();
                    if(hint != null){
                        hintsList.add(hint);
                    }
                }
            }
        }
        if(hintsList.isEmpty()){
            return null;
        }
        return hintsList;
    }


}
