package data.queryExecutors;

import data.FileReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DownloadsExecutor {

    private static final String INSERT_STATEMENT = "insert_downloads.sql";

    private static final int INSERT_USER_POSITION = 1;
    private static final int INSERT_SET_POSITION = 2;

    public static void insert(long userId, long setId, Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(INSERT_STATEMENT);
        PreparedStatement statement= connection.prepareStatement(query);
        statement.setLong(INSERT_USER_POSITION, userId);
        statement.setLong(INSERT_SET_POSITION, setId);
        statement.executeUpdate();
    }

    private static final String RATING_STATEMENT = "update_rating.sql";

    private static final int RATING_VALUE_POSITION = 1;
    private static final int RATING_USER_POSITION = 2;
    private static final int RATING_SET_POSITION = 3;

    public static void setRating(long userId, long setId, int rating, Connection connection) throws IOException, SQLException {
        String query = FileReader.readFile(RATING_STATEMENT);
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(RATING_VALUE_POSITION, rating);
        statement.setLong(RATING_USER_POSITION, userId);
        statement.setLong(RATING_SET_POSITION, setId);
        statement.executeUpdate();
    }
}
