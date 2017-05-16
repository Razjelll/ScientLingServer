package data.queryExecutors;

import data.DatabaseConnector;
import data.FileReader;
import data.models.Lesson;
import data.models.Set;

import javax.naming.NamingException;
import javax.xml.crypto.Data;
import java.io.IOException;
import java.sql.*;

public class InsertLessonExecutor {

    private final String INSERT_STATEMENT = "/insert_lesson.sql";

    private Connection mDbConnection;

    public InsertLessonExecutor(Connection connection) throws SQLException, NamingException, ClassNotFoundException {
        mDbConnection = connection;
    }

    private final int NAME_POS = 1;
    private final int NUMBER_POS = 2;
    private final int SET_POS = 3;

    public long insert(Lesson lesson) throws IOException, SQLException {
        String query = FileReader.readFile(INSERT_STATEMENT);

        /*Statement statements = mDbConnection.createStatement();
        statements.execute("SET CONSTRAINTS lessons_set_fk_fkey DEFERRED;");
        statements.close();*/
        PreparedStatement statement = mDbConnection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        statement.setString(NAME_POS, lesson.getName());
        statement.setInt(NUMBER_POS, lesson.getNumber());
        statement.setLong(SET_POS, lesson.getSetId());
        int insertedRows = statement.executeUpdate();
        /*long lessonId = -1;
        if(insertedRows == 1){
            ResultSet resultSet = statement.getGeneratedKeys();
            resultSet.next();
            lessonId =  resultSet.getLong(1);
            resultSet.close();
        }*/
        ResultSet  resultSet =  statement.getGeneratedKeys();
        long lessonId = -1;
        if(resultSet.next()){
            lessonId = resultSet.getLong(1);
        }
        resultSet.close();
        statement.close();
        return lessonId;
    }



}
