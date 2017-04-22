package data.queryBuilder;

import data.FileReader;

import java.io.IOException;

/**
 * Created by Razjelll on 20.04.2017.
 */
public class LessonsQueryCreator {
    private static final String FILE_PATH = "/lessons.sql";

    private static final String SET = "set_fk";

    public static String getQuery(long setId) throws IOException {
        String query = FileReader.readFile(FILE_PATH);
        QueryBuilder queryBuilder = new QueryBuilder(query);
        queryBuilder.addSelection(SET, setId, QueryBuilder.Sign.EQUAL);
        return queryBuilder.getQuery();
    }
}
