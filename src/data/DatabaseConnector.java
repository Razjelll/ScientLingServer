package data;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Razjelll on 12.04.2017.
 */
public class DatabaseConnector {

    private static final String JDBC = "jdbc/ScientLing2";

    public static Connection getConnection() throws ClassNotFoundException, SQLException, NamingException {
        InitialContext context = new InitialContext();
        DataSource dataSource = (DataSource)context.lookup(JDBC);
        Connection connection = dataSource.getConnection();
        return connection;

    }
}
