package data.queryExecutors;

import data.DatabaseConnector;
import data.FileReader;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Razjelll on 04.05.2017.
 */
public class CheckAuthorExecutor {

    private final String SELECT_STATEMENT = "check_author.sql";

    private Connection mDbConnection;

    public CheckAuthorExecutor() throws SQLException, NamingException, ClassNotFoundException {
        mDbConnection = DatabaseConnector.getConnection();
    }

    public CheckAuthorExecutor(Connection connection){
        mDbConnection = connection;
    }

    private final int ID_POS = 1;
    private final int AUTHOR_POS = 2;

    public boolean check(long setId, String author) throws IOException, SQLException {
        String query = FileReader.readFile(SELECT_STATEMENT);
        PreparedStatement statement = mDbConnection.prepareStatement(query);
        statement.setLong(ID_POS, setId);
        statement.setString(AUTHOR_POS, author);
        ResultSet resultSet = statement.executeQuery();
        if(resultSet.next()){
            //jeżeli znaleziono pasujący rekord
            if(resultSet.getInt(1)==1){
                return true;
            }
        }
        return false;
    }

    public void closeConnection() throws SQLException {
        mDbConnection.close();
    }
}
