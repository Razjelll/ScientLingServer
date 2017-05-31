package rest.responses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import data.DatabaseConnector;
import data.FileReader;
import data.files.MediaFileSystem;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class UsersSetsResponse {
    private static final String SELECT_QUERY = "select_user_sets.sql";

    private static final String ID = "id";
    private static final String NAME = "name";
    private static final String L1 = "l1";
    private static final String L2 = "l2";
    private static final String CATALOG = "catalog";
    private static final String IMAGES = "images";
    private static final String RECORDS = "records";

    private static final int USER_PARAM = 1;

    public static String create(String user) throws SQLException, NamingException, ClassNotFoundException, IOException {
        Connection connection = DatabaseConnector.getConnection();
        String query = FileReader.readFile(SELECT_QUERY);
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(USER_PARAM, user);
        ResultSet resultSet = statement.executeQuery();
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(getSetsRoot(resultSet,mapper));
    }

    private static ArrayNode getSetsRoot(ResultSet resultSet, ObjectMapper mapper) throws SQLException, IOException {
        ArrayNode root = mapper.createArrayNode();
        while(resultSet.next()){
            ObjectNode node = mapper.createObjectNode();
            node.put(ID, resultSet.getLong(ID));
            node.put(NAME, resultSet.getString(NAME));
            node.put(L1,resultSet.getLong(L1));
            node.put(L2, resultSet.getLong(L2));
            String catalog = resultSet.getString(CATALOG);
            boolean hasImages = MediaFileSystem.hasImages(catalog);
            boolean hasRecords = MediaFileSystem.hasRecords(catalog);
            node.put(IMAGES, hasImages);
            node.put(RECORDS, hasRecords);

            root.add(node);
        }
        return root;
    }
}
