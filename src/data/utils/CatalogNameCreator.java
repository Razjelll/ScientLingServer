package data.utils;


import data.DatabaseConnector;
import data.FileReader;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CatalogNameCreator {
    private static final String COUNT_CATALOG_STATEMENT = "count_catalog.sql";

    /**
     * Metoda wyznaczająca nazwę katalogu na obrazki i nagrania dla zbioru. Metoda sprawdza ile mnazw rozpoczynających się tak
     * jak nazwa zbioru istnieje w bazie i do nazwy wstawia liczbę o jeden większą niż znalezionych nazw katalogów.
     * To gwarantuje, że nazwa katalogu nie powtórzy się
     * @param rootName
     * @return
     * @throws SQLException
     * @throws NamingException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static synchronized String getName(String rootName) throws SQLException, NamingException, ClassNotFoundException, IOException {
        Connection connection = DatabaseConnector.getConnection();
        String query = FileReader.readFile(COUNT_CATALOG_STATEMENT);
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, rootName+"%");
        ResultSet resultSet = statement.executeQuery();
        if(resultSet.next()){
            int count = resultSet.getInt(1);
            if(count == 0){
                return rootName;
            } else {
                return rootName+(count+1);
            }
        }
        return null;
    }

    /**
     * Druga metoda wyznaczania nazwy katalogu.
     * Metod przyjmuje, że nazwa katalogu składa się z numeru id zestawu oraz jego nazwy
     * @param rootName
     * @param setId
     * @return
     */
    public static synchronized String getName(String rootName, long setId){
        return String.valueOf(setId) + "-" + rootName;
    }
}
