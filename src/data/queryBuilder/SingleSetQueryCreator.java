package data.queryBuilder;

import data.FileReader;

import java.io.IOException;

/**
 * Created by Razjelll on 20.04.2017.
 */
public class SingleSetQueryCreator {
    private static final String FILE_PATH = "/sets.sql";

    private static final String ID = "id";

    public static String getQuery(long setId) throws IOException {
        String query = FileReader.readFile(FILE_PATH);
        QueryBuilder builder = new QueryBuilder(query);
        builder.addSelection(ID, setId, QueryBuilder.Sign.EQUAL);
        return builder.getQuery();
    }
}
