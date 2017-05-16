package data.queryExecutors;

import data.FileReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DeleteExecutor {

    private static int SET_PARAM = 1;

    private static void delete(long setId, String queryPath, Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(queryPath);
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setLong(SET_PARAM, setId);
        statement.executeUpdate();
    }

    private static final String DELETE_SET = "delete_set.sql";
    public static void deleteSet(long setId, Connection connection) throws IOException, SQLException {
        delete(setId, DELETE_SET, connection);
    }
}
