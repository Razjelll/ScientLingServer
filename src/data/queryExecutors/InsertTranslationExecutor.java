package data.queryExecutors;

import data.FileReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class InsertTranslationExecutor {

    private final String INSERT_TRANSLATION = "insert_translation.sql";

    private PreparedStatement mStatement;

    public InsertTranslationExecutor(Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(INSERT_TRANSLATION);
        mStatement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    }

    private final int ITEM_POS = 1;
    private final int TRANSLATION_POS = 2;

    public boolean insert(long itemId, List<Long> translationsIds) throws SQLException {
        mStatement.setLong(ITEM_POS, itemId);
        boolean insertSuccess = false;
        for(Long translationId : translationsIds){
            insertSuccess = insert(translationId);
            if(!insertSuccess){
                return false;
            }
        }
        return true;
    }

    private boolean insert(long translationId) throws SQLException {
        mStatement.setLong(TRANSLATION_POS, translationId);
        int affectedRow = mStatement.executeUpdate();
        if(affectedRow == 0){
            return false;
        }
        return true;
    }

    public void close() throws SQLException {
        mStatement.close();
    }
}
