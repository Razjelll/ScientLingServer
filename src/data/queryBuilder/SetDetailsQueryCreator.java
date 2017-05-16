package data.queryBuilder;

import data.FileReader;

import java.io.IOException;


public class SetDetailsQueryCreator {
    private static final String FILE_PATH = "/set_details.sql";

    private static final String ID = "S.id";

    public static String getQuery(long setId) throws IOException {
        String query = FileReader.readFile(FILE_PATH);
        QueryBuilder builder = new QueryBuilder(query);
        builder.addSelection(ID, setId, QueryBuilder.Sign.EQUAL);
        return builder.getQuery();
    }
}
