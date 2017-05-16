package rest.responses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import data.DatabaseConnector;
import data.FileReader;
import data.files.MediaFileSystem;
import data.queryBuilder.SetDetailsQueryCreator;
import data.queryExecutors.SelectSetExecutor;
import data.queryExecutors.SetExecutor;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;


public class SetDetailsResponse {

    private static final String ID = "id";
    private static final String NAME = "name";
    private static final String L1 = "l1";
    private static final String L2 = "l2";
    private static final String AUTHOR = "author";
    private static final String DESCRIPTION = "description";
    private static final String WORDS_COUNT = "num_words";
    private static final String SIZE = "size";
    private static final String IMAGES_SIZE = "images_size";
    private static final String RECORDS_SIZE = "records_size";
    private static final String RATING = "rating";
    private static final String DOWNLOAD_COUNT = "download_count";
    private static final String ADDED_DATE = "added_date";
    private static final String USER_DOWNLOAD = "user_download";
    private static final String USER_RATING = "user_rating";


    private static final String SELECT_STATEMENT = "set_details.sql";
    public static String create(long setId, long userId) throws SQLException, NamingException, ClassNotFoundException, IOException {
        //String query = SetDetailsQueryCreator.getQuery(setId);
        String query = FileReader.readFile(SELECT_STATEMENT);
        Connection connection = DatabaseConnector.getConnection();
        ResultSet resultSet = SelectSetExecutor.getDetailsResultSet(setId, userId, connection);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = getResult(resultSet, connection, mapper);
        setSize(node, setId);
        connection.close();
        return mapper.writeValueAsString(node);
    }

    private static ObjectNode getResult(ResultSet resultSet, Connection connection, ObjectMapper mapper) throws SQLException {
        ObjectNode node= mapper.createObjectNode();
        if(resultSet.next()){
            node.put(ID, resultSet.getInt(ID));
            node.put(NAME, resultSet.getString(NAME));
            node.put(L1, resultSet.getString(L1));
            node.put(L2, resultSet.getString(L2));
            node.put(AUTHOR,resultSet.getString(AUTHOR));
            node.put(DESCRIPTION, resultSet.getString(DESCRIPTION));
            node.put(WORDS_COUNT, resultSet.getInt(WORDS_COUNT));
            node.put(SIZE, resultSet.getInt(SIZE));
            //node.put(IMAGES_SIZE, resultSet.getInt(IMAGES_SIZE));
            //node.put(RECORDS_SIZE, resultSet.getInt(RECORDS_SIZE));
            node.put(RATING, resultSet.getFloat(RATING));
            node.put(DOWNLOAD_COUNT, resultSet.getInt(DOWNLOAD_COUNT));
            if(resultSet.getDate(ADDED_DATE) != null){
                node.put(ADDED_DATE, resultSet.getDate(ADDED_DATE).toString());
            }
            node.put(USER_DOWNLOAD, resultSet.getBoolean(USER_DOWNLOAD));
            node.put(USER_RATING, resultSet.getInt(USER_RATING));

            return node;
        }
        return null;
    }

    private static void setSize(ObjectNode node, long setId) throws ClassNotFoundException, SQLException, IOException, NamingException {
        String catalog = getCatalog(setId);
        long imagesSize = MediaFileSystem.getImagesSize(catalog);
        long recordsSize = MediaFileSystem.getRecordsSize(catalog);
        node.put(IMAGES_SIZE, imagesSize);
        node.put(RECORDS_SIZE, recordsSize);
    }

    private static String getCatalog(long setId) throws SQLException, NamingException, ClassNotFoundException, IOException {
        Connection connection = DatabaseConnector.getConnection();
        SelectSetExecutor executor = new SelectSetExecutor(connection);
        String catalog = executor.getCatalog(setId);
        connection.close();
        return catalog;
    }

}
