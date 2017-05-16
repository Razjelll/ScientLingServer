package data.queryExecutors;

import data.FileReader;
import data.models.Word;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.*;

/**
 * Created by Razjelll on 05.05.2017.
 */
public class InsertItemExecutor {
    public final String INSERT_STATEMENT = "insert_item.sql";

    private PreparedStatement mStatement;

    public InsertItemExecutor(Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(INSERT_STATEMENT);
        mStatement =connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    }

    private final int WORD_POS = 1;
    private final int CATEGORY_POS = 2;
    private final int PART_OF_SPEECH_POS =3;
    private final int DIFFICULTY_POS = 4;
    private final int IMAGE_POS = 5;
    private final int RECORD_POS = 6;
    private final int LESSON_POS = 7;

    public long insert(Word word, long wordId) throws SQLException {
        prepareStatement(word, wordId);
        int affectedRows = mStatement.executeUpdate();
        ResultSet resultSet = mStatement.getGeneratedKeys();
        long itemId = -1;
        if(resultSet.next()){
            itemId = resultSet.getLong(1);
        }
        resultSet.close();
        return itemId;
    }

    private void prepareStatement(Word word, long wordId) throws SQLException {
        mStatement.clearParameters();
        mStatement.setLong(WORD_POS, wordId);

        if(word.getCategory() >0){
            mStatement.setLong(CATEGORY_POS, word.getCategory());
        } else {
            mStatement.setNull(CATEGORY_POS, Types.INTEGER);
        }

        if(word.getPartOfSpeech()>0){
            mStatement.setLong(PART_OF_SPEECH_POS, word.getPartOfSpeech());
        } else {
            mStatement.setNull(PART_OF_SPEECH_POS, Types.INTEGER);
        }

        if(word.getDifficulty()>0){
            mStatement.setInt(DIFFICULTY_POS, word.getDifficulty());
        } else {
            mStatement.setNull(DIFFICULTY_POS, Types.INTEGER);
        }

        if(word.getImage() != null){
            mStatement.setString(IMAGE_POS, word.getImage());
        } else {
            mStatement.setNull(IMAGE_POS, Types.VARCHAR);
        }

        if(word.getRecord() != null){
            mStatement.setString(RECORD_POS, word.getRecord());
        } else {
            mStatement.setNull(RECORD_POS, Types.VARCHAR);
        }

        mStatement.setLong(LESSON_POS, word.getLesson());
    }

    public void close() throws SQLException {
        mStatement.close();
    }
}
