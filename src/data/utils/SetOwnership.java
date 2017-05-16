package data.utils;


import data.DatabaseConnector;
import data.queryExecutors.CheckAuthorExecutor;
import rest.authentication.AuthenticationReader;

import javax.naming.NamingException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;

public class SetOwnership {

    public static boolean check(long setId, String authentication) throws IOException, SQLException, NamingException, ClassNotFoundException {
        Connection connection = DatabaseConnector.getConnection();
        String username = new AuthenticationReader(authentication).getUsername();
        boolean userIsOwner = new CheckAuthorExecutor(connection).check(setId, username);
        connection.close();
        return userIsOwner;
    }
}
