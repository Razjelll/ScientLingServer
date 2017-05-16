package data.queryExecutors;

import data.FileReader;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class InsertContentExecutor {

    private final String INSERT_STATEMENT = "/insert_word.sql";

    private PreparedStatement mStatement;

    public InsertContentExecutor(Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(INSERT_STATEMENT);
        mStatement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    }

    private final int CONTENT_POS = 1;

    public long insert(String content) throws IOException, SQLException {
        mStatement.setString(CONTENT_POS, content);
        int affectedRows = mStatement.executeUpdate();
        //if(affectedRows > 0){
            ResultSet resultSet = mStatement.getGeneratedKeys();
            long affectedId = -1;
            if(resultSet.next()){
                affectedId = resultSet.getLong(1);
            }
            resultSet.close();
            return affectedId;
        //}
        //return affectedId;
    }

    public List<Long> insert(List<String> translations) throws SQLException, IOException {
        List<Long> translationsIds = new ArrayList<>();
        for(String translation : translations){
           long id = insert(translation);
            if(id>0){
                translationsIds.add(id);
            }
        }
        return translationsIds;
    }

    public void close() throws SQLException {
        mStatement.close();
    }
}
