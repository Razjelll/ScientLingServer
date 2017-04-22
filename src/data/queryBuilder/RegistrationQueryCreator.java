package data.queryBuilder;

import data.FileReader;

import java.io.File;
import java.util.logging.Logger;

/**
 * Created by Razjelll on 20.04.2017.
 */
public class RegistrationQueryCreator {

    private static final String LOGIN = "login";
    private static final String EMAIL = "email";
    private static final String PASSWORD = "password";

    private static final String CHECK_QUERY = "SELECT COUNT(1) FROM Users";
    private static final String INSERT_STATEMENT = "INSERT INTO Users(" + LOGIN + "," + EMAIL + "," + PASSWORD + ") VALUES (?,?,?)";

    public static String getInsertQuery(String login, String email, String password){
        QueryBuilder builder = new QueryBuilder(INSERT_STATEMENT);
        builder.insertParam(login).insertParam(email).insertParam(password);
        return builder.getQuery();
    }

    public static String getCheckLoginQuery(String login){
        QueryBuilder builder = new QueryBuilder(CHECK_QUERY);
        builder.addSelection(LOGIN, login, QueryBuilder.Sign.EQUAL);

        return builder.getQuery();
    }

    public static String getCheckEmailQuery(String email){
        QueryBuilder builder = new QueryBuilder(CHECK_QUERY);
        builder.addSelection(EMAIL, email, QueryBuilder.Sign.EQUAL);
        return builder.getQuery();
    }

}
