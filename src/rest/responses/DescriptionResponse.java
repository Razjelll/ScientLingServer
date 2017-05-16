package rest.responses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import data.DatabaseConnector;
import data.queryExecutors.SelectSetExecutor;

import javax.naming.NamingException;
import javax.swing.plaf.nimbus.State;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DescriptionResponse {

    private static final String DESCRIPTION = "desc";

    public static String create(long setId) throws SQLException, NamingException, ClassNotFoundException, IOException {
        Connection connection = DatabaseConnector.getConnection();
        SelectSetExecutor executor = new SelectSetExecutor(connection);
        String description = executor.getDescription(setId);
        if(description == null){
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = getResult(description, mapper);
        return mapper.writeValueAsString(node);
    }

    private static ObjectNode getResult(String description, ObjectMapper mapper){
        ObjectNode node = mapper.createObjectNode();
        node.put(DESCRIPTION, description);
        return node;
    }

}
