package data.queryBuilder;

import data.FileReader;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by Razjelll on 20.04.2017.
 */
public class WordsQueryCreator {
    private static final String FILE_PATH = "/words.sql";

    private static final String SET = "L.set_fk";
    private static final String WORDS_COUNT = "words_count";

    public static String getQuery(long setId) throws IOException {
        String query = FileReader.readFile(FILE_PATH);
        QueryBuilder builder = new QueryBuilder(query);
        builder.addSelection(SET, setId, QueryBuilder.Sign.EQUAL);
        return builder.getQuery();
    }

    public static String getQuery(long setId, int offset, int limit) throws IOException {
        String query = FileReader.readFile(FILE_PATH);
        QueryBuilder builder = new QueryBuilder(query);
        builder.addSelection(SET, setId, QueryBuilder.Sign.EQUAL)
                .addOffset(offset).addLimit(limit);
        return builder.getQuery();
    }

    public static String getCountQuery(long setId){
        return "SELECT "+WORDS_COUNT+" FROM Sets WHERE id=" + setId;
    }
}
