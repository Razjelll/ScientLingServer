package data.queryExecutors;

import data.FileReader;
import data.models.SentenceEntry;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class InsertExemplarySentenceExecutor {
    private final String INSERT_STATEMENT = "insert_exemplary_sentence.sql";

    private PreparedStatement mStatement;

    public InsertExemplarySentenceExecutor(Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(INSERT_STATEMENT);
        mStatement = connection.prepareStatement(query);
    }

    private final int ITEM_POS = 1;
    private final int CONTENT_POS = 2;
    private final int TRANSLATION_POS = 3;

    public boolean insert(long itemId, List<SentenceEntry> sentencesIds) throws SQLException {
        mStatement.setLong(ITEM_POS, itemId);
        boolean insertSuccess = false;
        for(SentenceEntry entry: sentencesIds){
            insertSuccess = insert(entry);
            if(!insertSuccess){
                return false;
            }
        }
        return true;
    }

    private boolean insert(SentenceEntry entry) throws SQLException {
        if(entry.getContentId()>0){
            mStatement.setLong(CONTENT_POS, entry.getContentId());
            if(entry.getTranslationId()>0){
                mStatement.setLong(TRANSLATION_POS, entry.getTranslationId());
            } else {
                mStatement.setNull(TRANSLATION_POS, Types.INTEGER);
            }
            int affectedRows = mStatement.executeUpdate();
            if(affectedRows>0){
                return true;
            }
        }
        return false;
    }

    public void close() throws SQLException {
        mStatement.close();
    }
}
