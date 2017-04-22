package data.queryBuilder;

import javax.management.Query;
import java.util.logging.Logger;

/**
 * Created by Razjelll on 19.04.2017.
 */
public class QueryBuilder {

    public enum Sign {
        EQUAL("="),
        NON_EQUAl("<>"),
        SMALLER("<"),
        BIGGER(">"),
        LIKE(" LIKE ");

        private final String mValue;

        String getValue() {
            return mValue;
        }

        Sign(String value) {
            mValue = value;
        }
    }

    public enum Conjunction {
        AND(" AND "),
        OR(" OR ");

        private final String mValue;

        String getValue() {
            return mValue;
        }

        Conjunction(String value) {
            mValue = value;
        }
    }


    private StringBuilder mQueryBuilder;
    private StringBuilder mSelectionBuilder;
    private StringBuilder mLimitBuilder;
    private StringBuilder mOrderBuilder;
    /**Określa czy wstawiany warunek jest pierwszym warunkiem. Jeśli tak dodajemy wtedy WHERE, jeśli nie OR lub AND*/
    private boolean mFirstSelection;
    /**Określa czy wstawiamy pierwszą pozycję sortowania. Jeśli tak dodajemy ORDER BY */
    private boolean mFirstOrderBy;
    /**Określa czy w poprzednim dołączeniu do selection otwarto nawias. Jeśli nawias został otwarty do nastepnego warunku nie dodajemu OR ani AND
     * Zmienna będzie ustawiona na false przy pierwszym napotkanym dołączeniu warunku
     */
    private boolean mOpenedBarcket;

    public QueryBuilder(String query) {
        mQueryBuilder = new StringBuilder(query);
        mLimitBuilder = new StringBuilder();
        mFirstSelection = true;
        mFirstOrderBy = true;
        mOpenedBarcket = false;
    }

    public QueryBuilder addSelection(String column, String value, Sign sign, Conjunction conjunction, boolean lower) {
        addFirstSelection(conjunction);
        if (lower) {
            mSelectionBuilder.append(column).append(sign.getValue()).append("LOWER('").append(value).append("')");
        } else {
            mSelectionBuilder.append(column).append(sign.getValue()).append("'").append(value).append("'");
        }
        return this;
    }

    private void addFirstSelection(Conjunction conjunction) {
        if (mFirstSelection) {
            mSelectionBuilder = new StringBuilder();
            mSelectionBuilder.append(" WHERE ");
            mFirstSelection = false;
        } else {
            if(mOpenedBarcket){
                mOpenedBarcket = false;
            } else {
                mSelectionBuilder.append(conjunction.getValue());
            }
        }
    }

    public QueryBuilder addSelection(String column, String value, Sign sign, boolean lower) {
        return addSelection(column, value, sign, Conjunction.AND, lower);
    }

    public QueryBuilder addSelection(String column, String value, Sign sign) {
        return addSelection(column, value, sign, Conjunction.AND, false);
    }

    public QueryBuilder addSelection(String column, int value, Sign sign, Conjunction conjunction) {
        addFirstSelection(conjunction);
        addNumberSelection(column, value, sign);
        return this;
    }

    private void addNumberSelection(String column, long value, Sign sign) {
        mSelectionBuilder.append(column).append(sign.getValue()).append(value);
    }

    public QueryBuilder addSelection(String column, int value, Sign sign) {
        return addSelection(column, value, sign, Conjunction.AND);
    }

    public QueryBuilder addSelection(String column, long value, Sign sign, Conjunction conjunction) {
        addFirstSelection(conjunction);
        addNumberSelection(column, value, sign);
        return this;
    }

    public QueryBuilder addSelection(String column, long value, Sign sign) {
        return addSelection(column, value, sign, Conjunction.AND);
    }

    public QueryBuilder addLimit(int limit) {
        mLimitBuilder.append(" LIMIT ").append(String.valueOf(limit));
        return this;
    }

    public QueryBuilder addOffset(int offset) {
        mLimitBuilder.append("OFFSET ").append(String.valueOf(offset));
        return this;
    }

    public QueryBuilder addOrder(String orderBy) {
        if (mFirstOrderBy) {
            mOrderBuilder = new StringBuilder();
            mOrderBuilder.append(" ORDER BY ");
            mFirstOrderBy = false;
        } else {
            mOrderBuilder.append(", ");
        }
        mOrderBuilder.append(orderBy);
        return this;
    }

    public QueryBuilder openSelectionBracket(){
        addFirstBracket();
        mSelectionBuilder.append(" (");
        mOpenedBarcket = true;
        return this;
    }

    private void addFirstBracket(){
        if(mFirstSelection){
            mSelectionBuilder = new StringBuilder();
            mSelectionBuilder.append(" WHERE ");
            mFirstSelection = false;
        }
    }

    public QueryBuilder closeSelectionBracket(){
        addFirstBracket();
        mSelectionBuilder.append(") ");
        mOpenedBarcket = false;
        return this;
    }

    public String getQuery() {
        int questionIndex = mQueryBuilder.indexOf("?");
        //jeżeli znajdziem znak zapytania w jego miejsce wstawimy selection
        if (questionIndex > 0) {
            mQueryBuilder.delete(questionIndex, questionIndex + 1); //usuwamy znak zapytania
            if (mSelectionBuilder != null) {
                mQueryBuilder.insert(questionIndex, mSelectionBuilder.toString());
            }
        } else {
            if (mSelectionBuilder != null) {
                mQueryBuilder.append(mSelectionBuilder.toString());
            }
        }
        if (mOrderBuilder != null && mOrderBuilder.length() != 0) {
            mQueryBuilder.append(mOrderBuilder.toString());
        }
        if (mLimitBuilder.length() != 0) {
            mQueryBuilder.append(mLimitBuilder.toString());
        }
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe(mQueryBuilder.toString());
        return mQueryBuilder.toString();
    }

    public QueryBuilder insertParam(String param){
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe(mQueryBuilder.toString());
        int questionIndex = deleteQuestionMark();

        logger.severe(mQueryBuilder.toString());
        mQueryBuilder.insert(questionIndex,"'"+param+"'");
        return this;
    }

    public QueryBuilder insertParam(int param){
        int questionIndex = deleteQuestionMark();
        mQueryBuilder.insert(questionIndex,String.valueOf(param));
        return this;
    }

    private QueryBuilder insertParam(long param){
        int questionIndex = deleteQuestionMark();
        mQueryBuilder.insert(questionIndex,String.valueOf(param));
        return this;
    }

    private int deleteQuestionMark(){
        int questionIndex = mQueryBuilder.indexOf("?");
        if (questionIndex > 0) {
            mQueryBuilder.delete(questionIndex, questionIndex + 1); //usuwamy znak zapytania
        }
        return questionIndex;
    }
}
