package data.queryExecutors;

import data.models.*;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.*;
import java.util.List;


public class InsertWordExecutor {
    private final int FAILED = -1;

    private Connection mDbConnection;

    private InsertContentExecutor mContentExecutor;
    private InsertDefinitionExecutor mDefinitionExecutor;
    private InsertHintExecutor mHintExecutor;
    private InsertSentenceExecutor mSentenceExecutor;
    private InsertItemExecutor mItemExecutor;
    private InsertItemDefinitionExecutor mItemDefinitionExecutor;
    private InsertItemHintExecutor mItemHintsExecutor;
    private InsertExemplarySentenceExecutor mExemplarySentenceExecutor;
    private InsertTranslationExecutor mTranslationExecutor;

    public InsertWordExecutor(Connection connection) {
        mDbConnection = connection;
    }

    public long insert(Word word) throws SQLException, NamingException, ClassNotFoundException, IOException {

        //wstawiamy zawartość słówka
        long wordId = insertContent(word.getContent());
        if (wordId <= 0) {
            return FAILED;
        }
        //wstawiamy słówko
        long itemId = insertItem(word, wordId);
        if (itemId <= 0) {
            return FAILED;
        }
        //wstawiamy tłumaczenie do tabeli Words, a nastepnie wstawiamy rekordy do  tabeli Translations
        List<Long> translationsId = insertTranslationsWords(word.getTranslations());
        if (!insertTranslations(itemId, translationsId)) {
            return FAILED;
        }
        //jeżeli w modelu jest ustawiona definicja wstawiamy ją do tabeli Definitions i dodajemy rekord do tabeli ItemDefinitions
        if (word.getDefinition() != null) {
            DefinitionEntry definitionIds = insertDefinition(word.getDefinition());
            if (!insertItemDefinition(itemId, definitionIds)) {
                return FAILED; //jeżeli wstawianie się nie udało zwracamy -1 aby przerwać transakcję
            }
        }
        //jeżeli w modelu są zdania wstaiamy je do tabeli Sentences i wstawiamy rekordy do tabeli ExemplarySentence
        if (word.getSentences() != null && !word.getSentences().isEmpty()) {
            List<SentenceEntry> sentencesIds = insertSentences(word.getSentences());
            if (!insertExemplarySentence(itemId, sentencesIds)) {
                return FAILED;
            }
        }

        //jeżeli w modelu są podpowiedzi wstawiamy je do tabeli Hints i wstawiamy rekordy do tabeli ItemHints
        if (word.getHints() != null && !word.getHints().isEmpty()) {
            List<Long> hintsIds = insertHints(word.getHints());
            if (!insertItemHint(itemId, hintsIds)) {
                return FAILED;
            }
        }

        return -1;
    }

    private long insertContent(String content) throws IOException, SQLException {
        if (mContentExecutor == null) {
            mContentExecutor = new InsertContentExecutor(mDbConnection);
        }
        return mContentExecutor.insert(content);
    }

    private List<Long> insertTranslationsWords(List<String> translations) throws IOException, SQLException {
        if (mContentExecutor == null) {
            mContentExecutor = new InsertContentExecutor(mDbConnection);
        }
        return mContentExecutor.insert(translations);
    }

    private DefinitionEntry insertDefinition(Definition definition) throws IOException, SQLException {
        if (mDefinitionExecutor == null) {
            mDefinitionExecutor = new InsertDefinitionExecutor(mDbConnection);
        }
        return mDefinitionExecutor.insert(definition);
    }

    private List<Long> insertHints(List<String> hints) throws IOException, SQLException {
        if (mHintExecutor == null) {
            mHintExecutor = new InsertHintExecutor(mDbConnection);
        }
        return mHintExecutor.insert(hints);
    }

    private List<SentenceEntry> insertSentences(List<Sentence> sentences) throws IOException, SQLException {
        if (mSentenceExecutor == null) {
            mSentenceExecutor = new InsertSentenceExecutor(mDbConnection);
        }
        return mSentenceExecutor.insert(sentences);
    }

    private long insertItem(Word word, long wordId) throws IOException, SQLException {
        if (mItemExecutor == null) {
            mItemExecutor = new InsertItemExecutor(mDbConnection);
        }
        return mItemExecutor.insert(word, wordId);
    }


    private boolean insertTranslations(long itemId, List<Long> translationsIds) throws IOException, SQLException {
        if (mTranslationExecutor == null) {
            mTranslationExecutor = new InsertTranslationExecutor(mDbConnection);
        }
        return mTranslationExecutor.insert(itemId, translationsIds);
    }

    private boolean insertItemDefinition(long itemId, DefinitionEntry definitionEntry) throws IOException, SQLException {
        if (mItemDefinitionExecutor == null) {
            mItemDefinitionExecutor = new InsertItemDefinitionExecutor(mDbConnection);
        }
        return mItemDefinitionExecutor.insert(itemId, definitionEntry);
    }

    private boolean insertItemHint(long itemId, List<Long> hintsIds) throws IOException, SQLException {
        if (mItemHintsExecutor == null) {
            mItemHintsExecutor = new InsertItemHintExecutor(mDbConnection);
        }
        return mItemHintsExecutor.insert(itemId, hintsIds);
    }

    private boolean insertExemplarySentence(long itemId, List<SentenceEntry> sentencesIds) throws IOException, SQLException {
        if (mExemplarySentenceExecutor == null) {
            mExemplarySentenceExecutor = new InsertExemplarySentenceExecutor(mDbConnection);
        }
        return mExemplarySentenceExecutor.insert(itemId, sentencesIds);
    }


    public void close() throws SQLException {
        if (mContentExecutor != null) mContentExecutor.close();
        if (mDefinitionExecutor != null) mDefinitionExecutor.close();
        if (mHintExecutor != null) mHintExecutor.close();
        if (mSentenceExecutor != null) mSentenceExecutor.close();
        if (mItemExecutor != null) mItemExecutor.close();
        if (mTranslationExecutor != null) mTranslationExecutor.close();
        if (mItemDefinitionExecutor != null) mItemDefinitionExecutor.close();
        if (mItemHintsExecutor != null) mItemHintsExecutor.close();
        if (mExemplarySentenceExecutor != null) mExemplarySentenceExecutor.close();
    }
}
