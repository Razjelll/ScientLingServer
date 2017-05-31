package rest.responses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import data.DatabaseConnector;
import data.queryBuilder.LoginQueryCreator;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LoginResponse {

    private static final String LOGIN = "login";

    public static String create(String username, String password) throws ClassNotFoundException, SQLException, NamingException, IOException {
        String query = LoginQueryCreator.getQuery(username, password);
        Connection connection = DatabaseConnector.getConnection();
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = getResult(query, connection, mapper);
        return mapper.writeValueAsString(node);
    }

    private static ObjectNode getResult(String query, Connection connection, ObjectMapper mapper) throws SQLException {
        ObjectNode node = mapper.createObjectNode();

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        if(resultSet.next()){
            node.put(LOGIN, resultSet.getString(LOGIN));
        }
        return node;
    }
}
