package rest.responses;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import data.DatabaseConnector;
import data.queryBuilder.LessonsQueryCreator;
import data.queryBuilder.SingleSetQueryCreator;
import data.queryBuilder.WordsQueryCreator;
import sun.rmi.runtime.Log;

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
    private static final String DEFINITIONS = "definitions";
    private static final String DEFINITION_CONTENT = "c";
    private static final String DEFINITION_TRANSLATION = "t";
    private static final String CATEGORY = "category";
    private static final String PART_OF_SPEECH = "part";
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

    private static final String SET = "set";
    private static final String LESSONS = "lessons";
    private static final String WORDS = "words";

    public static String create(long setId) throws IOException, SQLException, NamingException, ClassNotFoundException, InterruptedException {
        /*String wordsQuery = WordsQueryCreator.getQuery(setId);
        String setQuery = SingleSetQueryCreator.getQuery(setId);
        String lessonsQuery = LessonsQueryCreator.getQuery(setId);*/

        Connection connection = DatabaseConnector.getConnection();
        ObjectMapper mapper = new ObjectMapper();
        //ArrayNode root = getRoot(wordsQuery,setQuery, connection, mapper);
        ObjectNode root =getRoot(setId, connection, mapper);

        return mapper.writeValueAsString(root);
    }

    private static ObjectNode getRoot(long setId, Connection connection, ObjectMapper mapper) throws SQLException, IOException, InterruptedException {
        ObjectNode root = mapper.createObjectNode();

        CountDownLatch countDownLatch = new CountDownLatch(3);

        final JsonNode[] nodes = new JsonNode[3];
        new Thread(() -> {
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
        }).start();

        countDownLatch.await();

        if(nodes[0] != null){
            root.set(SET, nodes[0]);
        }
        if(nodes[1] != null){
            root.set(LESSONS, nodes[1]);
        }
        if(nodes[2] != null){
            root.set(WORDS, nodes[2]);
        }

        return root;
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

    private static int getWordsCount(long setId, Connection connection) throws SQLException {
        String query = WordsQueryCreator.getCountQuery(setId);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        if(resultSet.next()){
            return resultSet.getInt(1);
        }
        return 0;
    }

    private static ArrayNode getWordsArray(long setId, Connection connection, ObjectMapper mapper) throws SQLException, IOException, InterruptedException {
        int wordsCount = getWordsCount(setId, connection);
        if(wordsCount == 0){
            return null;
        }
        int wordsPackSize = 0;
        int threadCount = THREAD_COUNT;
        if(wordsCount != 0){
            if(wordsCount >= THREAD_COUNT){
                wordsPackSize = wordsCount / THREAD_COUNT;
            } else {
                wordsPackSize = wordsCount;
                threadCount = wordsCount;
            }

            if(wordsCount % wordsPackSize != 0){
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
        ObjectMapper mapper = new ObjectMapper();

        while (resultSet.next()) {
            ObjectNode node = mapper.createObjectNode();

            node.put(CONTENT, resultSet.getString(CONTENT));

            translations = resultSet.getString(TRANSLATIONS);
            if (translations != null && !translations.isEmpty()) {
                ArrayNode translationsArray = node.putArray(TRANSLATIONS);
                putTranslations(translations, translationsArray, mapper);
            }

            definitions = resultSet.getString(DEFINITIONS);
            if (definitions != null && !definitions.isEmpty()) {
                ArrayNode definitionArray = node.putArray(DEFINITIONS);
                putDefinitions(definitions, definitionArray, mapper);
            }

            category = resultSet.getLong(CATEGORY);
            if (category > 0) {
                node.put(CATEGORY, category);
            }

            partOfSpeech = resultSet.getLong(PART_OF_SPEECH);
            if (partOfSpeech > 0) {
                node.put(PART_OF_SPEECH, partOfSpeech);
            }


            sentences = resultSet.getString(SENTENCES);
            if (sentences != null && !sentences.isEmpty()) {
                ArrayNode sentencesNode = node.putArray(SENTENCES);
                putSentences(sentences, sentencesNode, mapper);
            }

            hints = resultSet.getString(HINTS);
            if (hints != null && !hints.isEmpty()) {
                ArrayNode hintsNode = node.putArray(HINTS);
                putHints(hints, hintsNode, mapper);
            }

            image = resultSet.getString(IMAGE);
            if (image != null && !image.isEmpty()) {
                node.put(IMAGE, image);
            }

            record = resultSet.getString(RECORD);
            if (record != null && !record.isEmpty()) {
                node.put(RECORD, record);
            }

            node.put(LESSON, resultSet.getLong(LESSON));
            wordsArray.add(node);
        }
        return wordsArray;
    }

    private static void putTranslations(String translations, ArrayNode translationsNode, ObjectMapper mapper) {
        String[] translationsArray = translations.split(ELEMENT_SEPARATOR);
        for (String trans : translationsArray) {
            ObjectNode node = mapper.createObjectNode();
            node.put(TRANSLATION_CONTENT, trans);
            translationsNode.add(node);
        }
    }

    private static void putDefinitions(String definitions, ArrayNode definitionsNode, ObjectMapper mapper) {
        String[] definitionsArray = definitions.split(ELEMENT_SEPARATOR);
        for (String def : definitionsArray) {
            String[] definition = def.split(TRANSLATION_SEPARATOR);
            ObjectNode node = mapper.createObjectNode();
            node.put(DEFINITION_CONTENT, definition[0]);
            if (definition.length == 2) { //sprawdzamy czy definicja ma tłumaczenie
                node.put(DEFINITION_TRANSLATION, definition[1]);
            }
            definitionsNode.add(node);
        }
    }

    private static void putSentences(String sentences, ArrayNode sentencesNode, ObjectMapper mapper) {
        String[] sentencesArray = sentences.split(ELEMENT_SEPARATOR);
        for (String sent : sentencesArray) {
            String[] sentence = sent.split(TRANSLATION_SEPARATOR);
            ObjectNode node = mapper.createObjectNode();
            node.put(SENTENCE_CONTENT, sentence[0]);
            if (sentences.length() == 2) {
                node.put(SENTENCE_TRANSLATION, sentence[1]);
            }
            sentencesNode.add(node);
        }
    }

    private static void putHints(String hints, ArrayNode hintsNode, ObjectMapper mapper) {
        String[] hintsArray = hints.split(ELEMENT_SEPARATOR);
        for (String hint : hintsArray) {
            ObjectNode node = mapper.createObjectNode();
            node.put(HINT_CONTENT, hint);
            hintsNode.add(node);
        }
    }
}
