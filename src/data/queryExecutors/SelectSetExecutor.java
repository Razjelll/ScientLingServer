package data.queryExecutors;

import com.sun.xml.bind.v2.model.core.ID;
import data.DatabaseConnector;
import data.FileReader;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.*;

public class SelectSetExecutor {

    private final String CATALOG_STATEMENT = "select_catalog.sql";

    private Connection mDbConnection;

    public SelectSetExecutor() throws SQLException, NamingException, ClassNotFoundException {
        mDbConnection = DatabaseConnector.getConnection();
    }

    public SelectSetExecutor(Connection connection){
        mDbConnection = connection;
    }

    private final int ID_POS = 1;

    public String getCatalog(long setId) throws IOException, SQLException {
        String query = FileReader.readFile(CATALOG_STATEMENT);
        PreparedStatement statement = mDbConnection.prepareStatement(query);
        statement.setLong(ID_POS, setId);
        ResultSet resultSet = statement.executeQuery();
        if(resultSet.next()){
            return resultSet.getString(1);
        }
        return null;
    }

    private final String DESCRIPTION_STATEMENT = "select_description.sql";
    private final int ID_PARAM = 1;

    public String getDescription(long setId) throws IOException, SQLException {
        String query = FileReader.readFile(DESCRIPTION_STATEMENT);
        PreparedStatement statement = mDbConnection.prepareStatement(query);
        statement.setLong(ID_PARAM, setId);
        ResultSet resultSet = statement.executeQuery();
        String description = null;
        if(resultSet.next()){
            description = resultSet.getString(1);
        }
        resultSet.close();
        statement.close();
        return description;
    }

    private static final String DETAILS_STATEMENT = "set_details.sql";
    private static final int DETAILS_USER_POSITION =1;
    private static final int DETAILS_SET_POSITION =2;

    public static ResultSet getDetailsResultSet(long setId, long userId, Connection conenction) throws IOException, SQLException {
        String query = FileReader.readFile(DETAILS_STATEMENT);
        PreparedStatement statement  = conenction.prepareStatement(query);
        statement.setLong(DETAILS_USER_POSITION, userId);
        statement.setLong(DETAILS_SET_POSITION, setId);
        return statement.executeQuery();
    }

    public void closeConnection() throws SQLException {
        mDbConnection.close();
    }

}
