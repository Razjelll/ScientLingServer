package data.queryExecutors;

import data.models.Definition;
import data.models.DefinitionEntry;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class InsertDefinitionExecutor {
    private final String INSERT_STATEMENT = "/insert_definition.sql";

    private PreparedStatement mStatement;

    public InsertDefinitionExecutor(Connection connection) throws IOException, SQLException {
        String query = data.FileReader.readFile(INSERT_STATEMENT);
        mStatement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    }

    private final int CONTENT_POS = 1;

    public DefinitionEntry insert(Definition definition) throws SQLException {
        long definitionId = insert(definition.getContent());
        long translationId = -1;
        if(definitionId != -1 && definition.getTranslation() != null){
            translationId = insert(definition.getTranslation());
        }
        return new DefinitionEntry(definitionId, translationId);

    }

    private long insert(String content) throws SQLException {
        mStatement.setString(CONTENT_POS, content);
        mStatement.executeUpdate();
        ResultSet resultSet = mStatement.getGeneratedKeys();
        long result = -1;
        if(resultSet.next()){
           result =  resultSet.getLong(1);
        }
        resultSet.close();
        return result;
    }

    public void close() throws SQLException {
        mStatement.close();
    }


}
