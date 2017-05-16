package data.queryExecutors;

import data.FileReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UpdateSetExecutor {
    private final String UPDATE_WORDS_COUNT_STATEMENT = "/update_set_size.sql";

    private Connection mConnection;

    public UpdateSetExecutor(Connection connection){
        mConnection = connection;
    }

    private final int WORDS_COUNT_POS = 1;
    private final int SIZE_POS = 2;
    private final int SET_POS = 3;


    private class UpdateParm{
        private int mWordsCount;
        private int mSize;
        private int mImagesSize;
        private int mRecordsSize;

        public int getWordsCount(){return mWordsCount;}
        public int getSize(){return mSize;}
        public int getImagesSize(){return mImagesSize;}
        public int mRecordsSize(){return mRecordsSize;}

        public void setWordsCount(int wordsCount){mWordsCount = wordsCount;}
        public void setSize(int size){mSize =size;}
        public void setImagesSize(int size) {mImagesSize = size;}
        public void setRecordsSize(int size){mRecordsSize = size;}
    }

    public void updateWordsCount(long setId, int wordsCount, long size) throws IOException, SQLException {
        String query = FileReader.readFile(UPDATE_WORDS_COUNT_STATEMENT);
        PreparedStatement statement = mConnection.prepareStatement(query);
        statement.setInt(WORDS_COUNT_POS, wordsCount);
        statement.setLong(SIZE_POS, size);
        statement.setLong(SET_POS, setId);
        statement.executeUpdate();
    }

}
