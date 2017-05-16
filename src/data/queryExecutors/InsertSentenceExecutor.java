package data.queryExecutors;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.ResultTreeType;
import data.models.Sentence;
import data.models.SentenceEntry;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class InsertSentenceExecutor {

    private final String INSERT_STATEMENT = "insert_sentence.sql";

    private PreparedStatement mStatement;

    public InsertSentenceExecutor(Connection connection) throws IOException, SQLException {
        String query = data.FileReader.readFile(INSERT_STATEMENT);
        mStatement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    }

    public List<SentenceEntry> insert(List<Sentence> sentences) throws SQLException {
        List<SentenceEntry> sentencesIds = new ArrayList<>();
        for(Sentence sentence:sentences){
            long contentId = insert(sentence.getContent());
            long translationId = -1;
            if(contentId >0 && sentence.getTranslation() != null){
                translationId = insert(sentence.getTranslation());
            }
            sentencesIds.add(new SentenceEntry(contentId, translationId));
        }
        if(sentencesIds.isEmpty()){
            return null;
        }
        return sentencesIds;
    }

    private final int CONTENT_POS = 1;

    private long insert(String content) throws SQLException {
        mStatement.setString(CONTENT_POS, content);
        mStatement.executeUpdate();
        ResultSet resultSet = mStatement.getGeneratedKeys();
        long result = -1;
        if(resultSet.next()){
            result  = resultSet.getLong(1);
        }
        resultSet.close();
        return result;
    }

    public void close() throws SQLException {
        mStatement.close();
    }


}
