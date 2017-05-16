package data.queryExecutors;

import data.FileReader;
import data.models.DefinitionEntry;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;


public class InsertItemDefinitionExecutor {

    private final String INSERT_STATEMENT = "insert_item_definition.sql";

    private PreparedStatement mStatement;

    public InsertItemDefinitionExecutor(Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(INSERT_STATEMENT);
        mStatement = connection.prepareStatement(query);
    }

    private final int ITEM_POS = 1;
    private final int CONTENT_POS = 2;
    private final int TRANSLATION_POS = 3;

    public boolean insert(long itemId, DefinitionEntry entry) throws SQLException {
        if(itemId>0 && entry.getContentId()>0){
            mStatement.setLong(ITEM_POS, itemId);

            mStatement.setLong(CONTENT_POS, entry.getContentId());
            if(entry.getTranslationId()>0){
                mStatement.setLong(TRANSLATION_POS, entry.getTranslationId());
            } else {
                mStatement.setNull(TRANSLATION_POS, Types.INTEGER);
            }
            int affectedRows = mStatement.executeUpdate();
            if(affectedRows >0){
                return true;
            }
        }
        return false;
    }

    public void close() throws SQLException {
        mStatement.close();
    }

}
