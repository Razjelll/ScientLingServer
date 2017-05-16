package rest.responses;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import data.DatabaseConnector;
import data.queryBuilder.LessonsQueryCreator;
import data.queryBuilder.SingleSetQueryCreator;
import data.queryBuilder.WordsQueryCreator;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

public class DownloadSetResponse {

    private static int THREAD_COUNT = 4;

    private static final String ID = "id";
    private static final String NAME = "name";
    private static final String L1 = "l1";
    private static final String L2 = "l2";

    private static final String NUMBER = "number";

    private static final String CONTENT = "content";
    private static final String TRANSLATIONS = "translations";
    private static final String TRANSLATION_CONTENT = "c";
    private static final String DEFINITION = "definition";
    private static final String DEFINITION_CONTENT = "c";
    private static final String DEFINITION_TRANSLATION = "t";
    private static final String CATEGORY = "category";
    private static final String PART_OF_SPEECH = "part";
    private static final String DIFFICULTY = "difficulty";
    private static final String SENTENCES = "sentences";
    private static final String SENTENCE_CONTENT = "c";
    private static final String SENTENCE_TRANSLATION = "t";
    private static final String HINTS = "hints";
    private static final String HINT_CONTENT = "c";
    private static final String IMAGE = "image";
    private static final String RECORD = "record";
    private static final String LESSON = "lesson";

    private static final String TRANSLATION_SEPARATOR = "::";
    private static final String ELEMENT_SEPARATOR = ";";

    private static final String NUM_WORDS = "num_words";
    private static final String SET = "set";
    private static final String LESSONS = "lessons";
    private static final String WORDS = "words";

    private static final int SET_POSITION = 0;
    private static final int LESSONS_POSITION = 1;
    private static final int WORDS_POSITION = 2;

    private static final int NUM_NODES = 3;


    public static String create(long setId) throws IOException, SQLException, NamingException, ClassNotFoundException, InterruptedException {
        /*String wordsQuery = WordsQueryCreator.getQuery(setId);
        String setQuery = SingleSetQueryCreator.getQuery(setId);
        String lessonsQuery = LessonsQueryCreator.getQuery(setId);*/

        Connection connection = DatabaseConnector.getConnection();
        ObjectMapper mapper = new ObjectMapper();
        //ArrayNode root = getRoot(wordsQuery,setQuery, connection, mapper);
        ObjectNode root = getRoot(setId, connection, mapper);

        return mapper.writeValueAsString(root);
    }

    private static ObjectNode getRoot(long setId, Connection connection, ObjectMapper mapper) throws SQLException, IOException, InterruptedException {
        ObjectNode root = mapper.createObjectNode();

        CountDownLatch countDownLatch = new CountDownLatch(NUM_NODES);

        final JsonNode[] nodes = new JsonNode[NUM_NODES];

        int numWords = getNumWords(setId,connection);
        //TODO obsługa braku słówek do ppobrania
        //new Thread(new GetNodesRunnable(setId, NUM_WORDS, connection, mapper, nodes, countDownLatch)).start();
        GetNodesRunnable getWordsNodesRunnable = new GetNodesRunnable(setId, WORDS, connection, mapper, nodes, countDownLatch);
        getWordsNodesRunnable.setNumWords(numWords);
        new Thread(getWordsNodesRunnable).start();
        new Thread(new GetNodesRunnable(setId, SET,connection,mapper,nodes,countDownLatch)).start();
        new Thread(new GetNodesRunnable(setId, LESSON,connection,mapper,nodes,countDownLatch)).start();
       /* new Thread(() -> {
            try {
                nodes[2] = getWordsArray(setId, connection, mapper);
                countDownLatch.countDown();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            try {
                nodes[0] = getSetNode(setId, connection);
                countDownLatch.countDown();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(()->{
            try {
                nodes[1] = getLessonArray(setId, connection, mapper);
                countDownLatch.countDown();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();*/


        countDownLatch.await();

        root.put(NUM_WORDS, numWords);
        if(nodes[SET_POSITION] != null){
            root.set(SET, nodes[SET_POSITION]);
        }
        if(nodes[LESSONS_POSITION] != null){
            root.set(LESSONS, nodes[LESSONS_POSITION]);
        }
        if(nodes[WORDS_POSITION] != null){
            root.set(WORDS, nodes[WORDS_POSITION]);
        }

        return root;
    }

    private static  class GetNodesRunnable implements Runnable{

        private JsonNode[] mNodes;
        private long mSetId;
        private String mNode;
        private Connection mConnection;
        private ObjectMapper mMapper;
        private CountDownLatch mCountDownLatch;
        private int mNumWords;

        public GetNodesRunnable(long setId,String node, Connection connection, ObjectMapper mapper,
                                JsonNode[] nodesArray, CountDownLatch countDownLatch){
            mSetId = setId;
            mNode = node;
            mConnection = connection;
            mMapper = mapper;
            mCountDownLatch = countDownLatch;
            mNodes = nodesArray;
        }

        public void setNumWords(int numWords){
            mNumWords = numWords;
        }

        @Override
        public void run() {
            try {
                switch (mNode){
                    case SET:
                        mNodes[SET_POSITION] = getSetNode(mSetId, mConnection); break;
                    case LESSON:
                        mNodes[LESSONS_POSITION] = getLessonArray(mSetId, mConnection, mMapper); break;
                    case WORDS:
                        mNodes[WORDS_POSITION] = getWordsArray(mSetId,mNumWords, mConnection, mMapper); break;
                }
                mCountDownLatch.countDown();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static int getNumWords(long setId, Connection connection) throws SQLException {
        ResultSet resultSet = getNumWordsResultSet(setId, connection);
        if(resultSet.next()){
            return resultSet.getInt(1);
        }
        return 0;
    }

    private static ResultSet getNumWordsResultSet(long setId, Connection connection) throws SQLException {
        String query = WordsQueryCreator.getCountQuery(setId);
        Statement statement = connection.createStatement();
        return statement.executeQuery(query);
    }

    private static ObjectNode getSetNode(long setId, Connection connection) throws SQLException, IOException {
        ResultSet resultSet = getSetsResultSet(setId, connection);
        return createSetObjectNode(resultSet);
    }

    private static ResultSet getSetsResultSet(long setId, Connection connection) throws IOException, SQLException {
        String query = SingleSetQueryCreator.getQuery(setId);
        Statement statement = connection.createStatement();
        return statement.executeQuery(query);
    }

    private static ObjectNode createSetObjectNode(ResultSet resultSet) throws SQLException {
        if(resultSet.next()){
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode setNode = mapper.createObjectNode();
            setNode.put(ID, resultSet.getLong(ID));
            setNode.put(NAME, resultSet.getString(NAME));
            setNode.put(L1, resultSet.getLong(L1));
            setNode.put(L2, resultSet.getLong(L2));
            return setNode;
        } else {
            return null;
        }
    }

    private static ArrayNode getLessonArray(long setId, Connection connection, ObjectMapper mapper) throws SQLException, IOException {
        ResultSet resultSet = getLessonsResultSet(setId, connection);
        return createLessonsArrayNode(resultSet);
    }

    private static ResultSet getLessonsResultSet(long setId, Connection  connection) throws IOException, SQLException {
        String query = LessonsQueryCreator.getQuery(setId);
        Statement statement = connection.createStatement();
        return statement.executeQuery(query);
    }

    private static ArrayNode createLessonsArrayNode(ResultSet resultSet) throws SQLException {
        if(!resultSet.isBeforeFirst()){
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = mapper.createArrayNode();
        while(resultSet.next()){
            ObjectNode node = mapper.createObjectNode();
            node.put(ID, resultSet.getLong(ID));
            node.put(NAME, resultSet.getString(NAME));
            node.put(NUMBER, resultSet.getInt(NUMBER));
            arrayNode.add(node);
        }
        return arrayNode;
    }

    private static ArrayNode getWordsArray(long setId,int numWords, Connection connection, ObjectMapper mapper) throws SQLException, IOException, InterruptedException {
        int wordsPackSize = 0;
        int threadCount = THREAD_COUNT;
        if(numWords != 0){
            if(numWords >= THREAD_COUNT){
                wordsPackSize = numWords / THREAD_COUNT;
            } else {
                wordsPackSize = numWords;
                threadCount = numWords;
            }

            if(numWords % wordsPackSize != 0){
                wordsPackSize++;
            }
        }

        final int packSize = wordsPackSize;
        final ArrayNode wordsArray = mapper.createArrayNode();
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        for(int i=0; i<threadCount; i++){
            final int page = i;
            new Thread(new CreateWordsArrayThread(wordsArray,setId,page, packSize,connection, countDownLatch)).start();
        }
        countDownLatch.await();
        return wordsArray;
        //return createWordsArrayNode(resultSet);
    }

    private static class CreateWordsArrayThread implements Runnable{

        private ArrayNode wordsArray;
        private int page;
        private  int limit;
        private long setId;
        private Connection connection;
        private CountDownLatch countDownLatch;

        public CreateWordsArrayThread(ArrayNode wordsArray,Long setId, int page, int limit, Connection connection, CountDownLatch countDownLatch){
            this.wordsArray = wordsArray;
            this.page = page;
            this.limit = limit;
            this.setId = setId;
            this.connection = connection;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                ObjectMapper mapper = new ObjectMapper();
                final ResultSet resultSet = getWordsResultSet(setId, page*limit, limit, connection);
                createWordsArrayNode(resultSet, wordsArray);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    private static ResultSet getWordsResultSet (long setId,int offset, int limit, Connection connection) throws IOException, SQLException {
        String query = WordsQueryCreator.getQuery(setId, offset, limit);
        Statement statement = connection.createStatement();
        return statement.executeQuery(query);
    }

    private static ArrayNode createWordsArrayNode(ResultSet resultSet, final ArrayNode wordsArray) throws SQLException {
        if(!resultSet.isBeforeFirst()){
            return null;
        }

        String translations, definitions, sentences, hints, image, record;
        long category, partOfSpeech;
        int difficulty;
        ObjectMapper mapper = new ObjectMapper();

        while (resultSet.next()) {
            ObjectNode node = mapper.createObjectNode();

            node.put(CONTENT, resultSet.getString(CONTENT));

            translations = resultSet.getString(TRANSLATIONS);
            if(translations != null && !translations.isEmpty()){
                putTranslations(translations, node, mapper);
            }

            sentences=resultSet.getString(SENTENCES);
            if(sentences != null && !sentences.isEmpty()){
                putSentences(sentences,node,mapper);
            }

            definitions = resultSet.getString(DEFINITION);
            if(definitions != null && !definitions.isEmpty() ){
                putDefinitions(definitions, node);
            }

            category = resultSet.getLong(CATEGORY);
            if(category >0){
                node.put(CATEGORY, category);
            }
            partOfSpeech = resultSet.getLong(PART_OF_SPEECH);
            if(partOfSpeech>0) {
                node.put(PART_OF_SPEECH, partOfSpeech);
            }

            difficulty = resultSet.getInt(DIFFICULTY);
            if(difficulty > 0){
                node.put(DIFFICULTY, difficulty);
            }

            sentences = resultSet.getString(SENTENCES);
            if(sentences != null && !sentences.isEmpty()){
                putSentences(sentences, node,mapper);
            }

            hints = resultSet.getString(HINTS);
            if(hints != null && !hints.isEmpty()){
                putHints(hints, node, mapper);
            }

            image = resultSet.getString(IMAGE);
            if(image != null && !image.isEmpty()) {
                node.put(IMAGE, image);
            }

            record = resultSet.getString(RECORD);
            if(record!= null && !record.isEmpty()){
                node.put(RECORD, record);
            }

            node.put(LESSON, resultSet.getLong(LESSON));
            wordsArray.add(node);
        }
        return wordsArray;
    }

    private static void putTranslations(String translations, ObjectNode node, ObjectMapper mapper) throws SQLException {
            ArrayNode translationsNode = node.putArray(TRANSLATIONS);
            putElements(translations, TRANSLATION_CONTENT, translationsNode, mapper);
    }

    private static void putElements(String element, String contentName, ArrayNode arrayNode, ObjectMapper mapper){
        String[] elementArray = element.split(ELEMENT_SEPARATOR);
        for (String trans : elementArray) {
            ObjectNode node = mapper.createObjectNode();
            node.put(contentName, trans);
            arrayNode.add(node);
        }
    }

    private static void putDefinitions(String definition, ObjectNode node) throws SQLException {
        //putElementsWithTranslation(definitions, DEFINITION_CONTENT, DEFINITION_TRANSLATION, definitionsNode, mapper);
        ObjectNode definitionNode = node.putObject(DEFINITION);
        String[] definitionParts = definition.split(TRANSLATION_SEPARATOR);
        definitionNode.put(DEFINITION_CONTENT, definitionParts[0]);
        if(definitionParts.length == 2){
            definitionNode.put(DEFINITION_TRANSLATION, definitionParts[1]);
        }
    }

    private static void putSentences(String sentences, ObjectNode node, ObjectMapper mapper) {
        ArrayNode sentencesNode = node.putArray(SENTENCES);
        //putElementsWithTranslation(sentences, SENTENCE_CONTENT, SENTENCE_TRANSLATION, sentencesNode, mapper);
        String[] sentenceParts = sentences.split(ELEMENT_SEPARATOR);
        for (String elem : sentenceParts) {
            String[] elementWithTranslation = elem.split(TRANSLATION_SEPARATOR);
            ObjectNode translationNode = mapper.createObjectNode();
            translationNode.put(SENTENCE_CONTENT, elementWithTranslation[0]);
            if (elementWithTranslation.length == 2) { //sprawdzamy czy definicja ma tłumaczenie
                translationNode.put(SENTENCE_TRANSLATION, elementWithTranslation[1]);
            }
            sentencesNode.add(translationNode);
        }
    }

    private static void putHints(String hints, ObjectNode node, ObjectMapper mapper) {
        ArrayNode hintsNode = node.putArray(HINTS);
        putElements(hints, HINT_CONTENT, hintsNode, mapper);
    }
}
