package data.queryExecutors;

import data.FileReader;
import data.models.Set;
import data.utils.CatalogNameCreator;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class SetExecutor {

    private final String INSERT_STATEMENT = "/insert_set.sql";

    private Connection mConnection;

    public SetExecutor(Connection connection){
        mConnection = connection;
    }

    private final int NAME_POS = 1;
    private final int CATALOG_POS = 2;
    private final int DESCRIPTION_POS = 3;
    private final int L1_POS = 4;
    private final int L2_POS = 5;
    private final int AUTHOR_POS = 6;
    private final int ADDED_DATE = 7;

    public long insert(Set set) throws IOException, SQLException, ClassNotFoundException, NamingException {
        String query = FileReader.readFile(INSERT_STATEMENT);
        PreparedStatement statement = mConnection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        statement.setString(NAME_POS, set.getName());
        String catalogName = CatalogNameCreator.getName(set.getName());
        statement.setString(CATALOG_POS, catalogName);
        if(set.getDescription() != null){
            statement.setString(DESCRIPTION_POS, set.getDescription());
        }
        statement.setLong(L1_POS, set.getL1());
        statement.setLong(L2_POS, set.getL2());
        statement.setLong(AUTHOR_POS, set.getAuthorId());
        Date sqlDate = new Date(new java.util.Date().getTime());
        statement.setDate(ADDED_DATE,sqlDate);
        int affectedRows = statement.executeUpdate();
        long setId = -1;
        if(affectedRows>0){
            ResultSet resultSet = statement.getGeneratedKeys();
            if(resultSet.next()){
                setId = resultSet.getInt(1);
                resultSet.close();
            }
        }
        statement.close();

        return setId;
    }

    private final String DESCRIPTION_STATEMENT = "update_description.sql";

    private final int DESCRIPTION_PARAM = 1;
    private final int ID_PARAM = 2;

    public void updateDescription(long setId, String description) throws IOException, SQLException {
        String query = FileReader.readFile(DESCRIPTION_STATEMENT);
        PreparedStatement statement = mConnection.prepareStatement(query);
        statement.setString(DESCRIPTION_PARAM, description);
        statement.setLong(ID_PARAM,setId );
        statement.executeUpdate();
    }


    private final String DOWNLOAD_COUNT_STATEMENT = "increase_download_count.sql";

    public void increaseDownloadCount(long setId) throws IOException, SQLException {
        String query = FileReader.readFile(DOWNLOAD_COUNT_STATEMENT);
        PreparedStatement statement = mConnection.prepareStatement(query);
        statement.setLong(1, setId);
        statement.executeUpdate();
    }
}
