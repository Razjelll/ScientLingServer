package rest.responses;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import data.DatabaseConnector;
import data.queryBuilder.RegistrationQueryCreator;

import javax.naming.NamingException;
import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * Created by Razjelll on 20.04.2017.
 */
public class RegistrationResponse {

    public static Response create(String login, String email, String password) throws SQLException, NamingException, ClassNotFoundException, JsonProcessingException {
        Logger logger = Logger.getLogger("RegistrationResponse");
        String checkLoginQuery = RegistrationQueryCreator.getCheckLoginQuery(login);
        String checkEmailQuery = RegistrationQueryCreator.getCheckEmailQuery(email);
        Connection connection = DatabaseConnector.getConnection();
        boolean loginExist = check(checkLoginQuery, connection);
        boolean emailExist = check(checkEmailQuery, connection);
        logger.severe(checkLoginQuery);
        logger.severe(checkEmailQuery);
        logger.severe(loginExist + " "  + emailExist);

        if(loginExist && emailExist){
            String insertQuery = RegistrationQueryCreator.getInsertQuery(login, email, password);
            int insertedItems = insert(insertQuery, connection);
            if(insertedItems == 1){
                return Response.ok().build();
            }
        } else {
            String errorResponse = createErrorResponse(loginExist, emailExist);

            return Response.status(412).entity(errorResponse).build();
        }

        return null;
    }

    private static final String ERROR = "error";

    private static String createErrorResponse(boolean loginExist, boolean emailExist) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode root = mapper.createArrayNode();
        if(!loginExist){
            ObjectNode node = mapper.createObjectNode();
            node.put(ERROR, "login");
            root.add(node);
        }
        if(!emailExist){
            ObjectNode node = mapper.createObjectNode();
            node.put(ERROR, "email");
            root.add(node);
        }
        return mapper.writeValueAsString(root);
    }

    private static int insert(String query, Connection connection) throws SQLException{
        Statement statement = connection.createStatement();
        int resultSet = statement.executeUpdate(query);
        return resultSet;
    }

    private static boolean check(String query, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        if(resultSet.next()){
            int count = resultSet.getInt(1);
            if(count == 0){
                return true;
            }
        }
        return false;
    }
}
