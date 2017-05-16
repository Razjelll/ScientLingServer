package data.queryExecutors;

import data.FileReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

//TODO zorientować się czy ta klasa jest potrzebna. Chyba można to załatwić za pomoca InsertContentExecutor
public class InsertWordTranslationsExecutor {
    private final String INSERT_STATEMENT = "/insert_word.sql";

    private PreparedStatement mStatement;

    public InsertWordTranslationsExecutor(Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(INSERT_STATEMENT);
        mStatement = connection.prepareStatement(query);
    }

    private final int CONTENT_POS = 1;

    public List<Long> insert(List<String> translations) throws SQLException {
        List<Long> translationsIds = new ArrayList<>();
        for(String translation : translations){
            mStatement.setString(CONTENT_POS, translation);
            mStatement.executeUpdate();
            ResultSet resultSet = mStatement.getGeneratedKeys();
            if(resultSet.next()){
                translationsIds.add(resultSet.getLong(1));
            }
            resultSet.close();
        }
        return translationsIds;
    }

    public void close() throws SQLException {
        mStatement.close();
    }
}
