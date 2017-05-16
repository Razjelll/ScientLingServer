package data.queryExecutors;

import data.FileReader;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class InsertHintExecutor {
    public final String INSERT_STATEMENT = "insert_hint.sql";

    private PreparedStatement mStatement;

    public InsertHintExecutor(Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(INSERT_STATEMENT);
        mStatement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    }

    private final int CONTENT_POS = 1;

    public List<Long> insert(List<String> hints) throws SQLException {
        List<Long> hintsIds = new ArrayList<>();
        for(String hint:hints){
            mStatement.setString(CONTENT_POS, hint);
            mStatement.executeUpdate();
            ResultSet resultSet = mStatement.getGeneratedKeys();
            if(resultSet.next()){
                hintsIds.add(resultSet.getLong(1));
            }
            resultSet.close();
        }
        if(hintsIds.isEmpty()){
            return null;
        }
        return hintsIds;
    }

    public void close() throws SQLException {
        mStatement.close();
    }
}
