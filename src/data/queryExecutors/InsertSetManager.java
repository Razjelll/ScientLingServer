package data.queryExecutors;

import data.DatabaseConnector;
import data.models.Lesson;
import data.models.Set;
import data.models.Word;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;


public class InsertSetManager {
    private Connection mDbConnection;
    private SetExecutor mSetExecutor;
    private InsertLessonExecutor mLessonExecutor;
    private InsertWordExecutor mWordExecutor;
    private UpdateSetExecutor mUpdateExecutor;

    private int mWordCount;

    public InsertSetManager() throws SQLException, NamingException, ClassNotFoundException {
        mDbConnection = DatabaseConnector.getConnection();
        mSetExecutor = new SetExecutor(mDbConnection);
        mLessonExecutor = new InsertLessonExecutor(mDbConnection);
        mWordExecutor = new InsertWordExecutor(mDbConnection);
        mUpdateExecutor = new UpdateSetExecutor(mDbConnection);
    }

    public void beginTransaction() throws SQLException {
        //metoda rozpoczynająca transackję
        mDbConnection.setAutoCommit(false);
    }

    public void endFailedTransaction() throws SQLException {
        mDbConnection.rollback();
        mDbConnection.setAutoCommit(true);
    }

    public void endSuccessfulTransaction() throws SQLException {
        //zatwierdzenie transakcji
        mDbConnection.commit();
        mDbConnection.setAutoCommit(true);
    }

    public long insertSet(Set set) throws IOException, SQLException, NamingException, ClassNotFoundException {
        return mSetExecutor.insert(set);
    }

    public long insertLesson(Lesson lesson) throws IOException, SQLException {
        return mLessonExecutor.insert(lesson);
    }

    public long insertWord(Word word) throws ClassNotFoundException, SQLException, IOException, NamingException {
        mWordCount++;
        return mWordExecutor.insert(word);
    }

    public void updateWordsCount(long setId, long size) throws IOException, SQLException {
        mUpdateExecutor.updateWordsCount(setId, mWordCount, size);
    }


}
