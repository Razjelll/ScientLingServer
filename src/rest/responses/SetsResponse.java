package rest.responses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import data.DatabaseConnector;
import data.queryBuilder.SetsQueryCreator;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Razjelll on 19.04.2017.
 */
public class SetsResponse {

    private static final String ID = "id";
    private static final String NAME = "name";
    private static final String WORDS_COUNT = "words_count";
    private static final String SIZE = "size";
    private static final String DESCRIPTION = "description";
    private static final String AUTHOR = "author";
    private static final String RATING = "rating";
    private static final String DOWNLOADS = "downloads";
    private static final String IMAGES_SIZE = "images_size";
    private static final String RECORDS_SIZE = "records_size";
    private static final String LANGUAGE_L1 = "l1";
    private static final String LANGUAGE_L2 = "l2";

    public static String create(String name, long l1, long l2, int sorting, int page, int limit ) throws IOException, SQLException, NamingException, ClassNotFoundException {

        String query = SetsQueryCreator.getQuery(name, l1, l2, sorting, page, limit);
        Connection connection = DatabaseConnector.getConnection();
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode root = getSetsRoot(query,connection, mapper);
        return mapper.writeValueAsString(root);
    }

    private static ArrayNode getSetsRoot(String query, Connection connection, ObjectMapper mapper) throws SQLException, IOException {
        ArrayNode root = mapper.createArrayNode();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        while(resultSet.next()){
            ObjectNode node = mapper.createObjectNode();
            node.put(ID, resultSet.getLong(ID));
            node.put(NAME, resultSet.getString(NAME));
            node.put(WORDS_COUNT, resultSet.getInt(WORDS_COUNT));
            node.put(SIZE, resultSet.getInt(SIZE));
            node.put(DESCRIPTION, resultSet.getString(DESCRIPTION));
            node.put(AUTHOR, resultSet.getString(AUTHOR));
            node.put(RATING, resultSet.getFloat(RATING));
            node.put(DOWNLOADS, resultSet.getInt(DOWNLOADS));
            node.put(IMAGES_SIZE, resultSet.getInt(IMAGES_SIZE));
            node.put(RECORDS_SIZE, resultSet.getInt(RECORDS_SIZE));
            node.put(LANGUAGE_L2, resultSet.getString(LANGUAGE_L1));
            node.put(LANGUAGE_L1, resultSet.getString(LANGUAGE_L2));
            root.add(node);
        }

        return root;
    }
}
