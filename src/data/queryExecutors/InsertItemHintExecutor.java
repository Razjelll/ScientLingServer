package data.queryExecutors;

import data.FileReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class InsertItemHintExecutor {
    private final String INSERT_STATEMENT = "insert_item_hint.sql";

    private PreparedStatement mStatement;

    public InsertItemHintExecutor(Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(INSERT_STATEMENT);
        mStatement = connection.prepareStatement(query);
    }

    private final int ITEM_POS = 1;
    private final int HINT_POS = 2;

    public boolean insert(long itemId, List<Long> hintsIds) throws SQLException {
        if(itemId>0){
            mStatement.setLong(ITEM_POS, itemId);
            boolean insertSuccess = false;
            for(long hintId : hintsIds){
                insertSuccess = insert(hintId);
                if(!insertSuccess){
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private boolean insert(long hintId) throws SQLException {
        mStatement.setLong(HINT_POS, hintId);
        int affectedRows = mStatement.executeUpdate();
        if(affectedRows>0){
            return true;
        }
        return false;
    }

    public void close() throws SQLException {
        mStatement.close();
    }
}
