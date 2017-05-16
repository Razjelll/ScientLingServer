package data.queryExecutors;

import data.DatabaseConnector;
import data.FileReader;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SelectUserIdExecutor {
    private static final String USER_ID_STATEMENT = "select_user_id.sql";
    private static final int USERNAME_PARAM = 1;
    public static long getUserId(String username) throws SQLException, NamingException, ClassNotFoundException, IOException {
        Connection connection = DatabaseConnector.getConnection();
        String query = FileReader.readFile(USER_ID_STATEMENT);
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(USERNAME_PARAM, username);
        ResultSet resultSet = statement.executeQuery();
        long userId = -1;
        if(resultSet.next()){
            userId = resultSet.getLong(1);
        }
        statement.close();
        connection.close();
        return userId;
    }
}
