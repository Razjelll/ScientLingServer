package data.queryBuilder;

import javax.management.Query;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

/**
 * Created by Razjelll on 19.04.2017.
 */
public class SetsQueryCreator {
    private static final String FILE_PATH = "/select_sets.sql";
    private static final String NAME = "S.name";
    private static final String L1 = "S.languagel1_fk";
    private static final String L2 = "S.languagel2_fk";

    private static final String[] ORDER_BY = {"", "download_count DESC","rating DESC", "added_date DESC" };

    public static String getQuery(String name, long l1, long l2, int sorting, int page, int limit) throws IOException {
        String query = data.FileReader.readFile(FILE_PATH);
        QueryBuilder queryBuilder = new QueryBuilder(query);
        if(name != null && !name.isEmpty()){
            queryBuilder.addSelection("LOWER("+NAME+")","%"+name+"%", QueryBuilder.Sign.LIKE, true);
        }
        if(l1 > 0){
            queryBuilder.addSelection(L1, l1, QueryBuilder.Sign.EQUAL);
        }
        if(l2 > 0){
            queryBuilder.addSelection(L2, l2, QueryBuilder.Sign.EQUAL);
        }
        if(sorting > 0){
            queryBuilder.addOrder(ORDER_BY[sorting]);
        }
        if(page >0){
            queryBuilder.addOffset(page*limit);
        }
        if(limit > 0){
            queryBuilder.addLimit(limit);
        }

        Logger logger = Logger.getLogger("dasdas");
        logger.severe(queryBuilder.toString());
        return queryBuilder.getQuery();
    }
}
