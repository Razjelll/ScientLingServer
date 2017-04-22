package data.queryBuilder;

import data.DatabaseConnector;
import data.FileReader;
import sun.rmi.runtime.Log;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Created by Razjelll on 19.04.2017.
 */
public class LoginQueryCreator {
    private static final String FILE_PATH = "/login.sql";

    private static final String LOGIN = "login";
    private static final String EMAIL = "email";
    private static final String PASSWORD = "password";

    public static String getQuery(String username, String password) throws IOException, SQLException, NamingException, ClassNotFoundException {
        String query = FileReader.readFile(FILE_PATH);
        QueryBuilder builder = new QueryBuilder(query);
        builder.openSelectionBracket()
                .addSelection("LOWER("+LOGIN+")", username, QueryBuilder.Sign.EQUAL, true)
                .addSelection("LOWER("+EMAIL+")", username, QueryBuilder.Sign.EQUAL, QueryBuilder.Conjunction.OR, true)
                .closeSelectionBracket()
                .addSelection(PASSWORD, password, QueryBuilder.Sign.EQUAL);
        return builder.getQuery();
    }
}
