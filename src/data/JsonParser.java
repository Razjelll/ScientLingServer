package data;

import com.fasterxml.jackson.core.*;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

public class JsonParser {

    private com.fasterxml.jackson.core.JsonParser mParser;
    private long mJsonLength;

    public JsonParser(InputStream inputStream) throws IOException {
        JsonFactory jsonFactory = new JsonFactory();
        mParser = jsonFactory.createParser(inputStream);
    }

    public void close() throws IOException {
        mParser.close();
    }

    public JsonToken nextToken() throws IOException {
        JsonToken token = mParser.nextToken();
        if(token != null){
            switch (token){
                case START_ARRAY:
                case START_OBJECT:
                case END_ARRAY:
                case END_OBJECT:
                    mJsonLength+=1; //wszystkie te tokeny zaweirają jeden znak ( {, [,},} )
            }
        }
        return token;
    }

    public JsonToken nextValue() throws IOException {
        JsonToken token = mParser.nextValue();
        if(token != null){
            mJsonLength+=token.toString().length();
        }
        return token;
    }


    public JsonParser skipChildren() throws IOException {
        mParser.skipChildren();
        return this;
    }

    public boolean isClosed() {
        return mParser.isClosed();
    }

    public JsonToken getCurrentToken() {
        JsonToken token = mParser.getCurrentToken();
        if(token != null){
            mJsonLength+=token.toString().length();
        }
        return token;
    }


    public boolean hasCurrentToken() {
        return mParser.hasCurrentToken();
    }

    public boolean hasTokenId(int i) {
        return mParser.hasTokenId(i);
    }

    public boolean hasToken(JsonToken jsonToken) {
        return  mParser.hasToken(jsonToken);
    }


    public String getCurrentName() throws IOException {
        String name = mParser.getCurrentName();
        if(name != null){
            mJsonLength+=name.length() + 2 + 1; //2 = "" 1 = :
        }
        return name;
    }

    public JsonLocation getCurrentLocation() {
        return mParser.getCurrentLocation();
    }


    public String getText() throws IOException {
        String text = mParser.getText();
        if(text != null){
            mJsonLength+=text.length() + 2; //ponieważ doliczmy nawiasy
        }
        return text;
    }

    public String nextTextValue() throws IOException {
        String text = mParser.nextTextValue();
        if(text != null){
            mJsonLength+=text.length() + 2 ;//ponieważ doliczamy nawiasy
        }
        return text;
    }


    public int getTextLength() throws IOException {
        return mParser.getTextLength();
    }

    public int getTextOffset() throws IOException {
        return mParser.getTextOffset();
    }



    public Number getNumberValue() throws IOException {
        Number number = mParser.getNumberValue();
        mJsonLength += String.valueOf(number).length();
        return number;
    }




    public int getIntValue() throws IOException {
        int number = mParser.getIntValue();
        mJsonLength+=String.valueOf(number).length();
        return number;
    }

    public int nextIntValue(int defaultValue) throws IOException {
        int number = mParser.nextIntValue(defaultValue);
        mJsonLength+=String.valueOf(number).length();
        return number;
    }


    public long getLongValue() throws IOException {
        long number = mParser.getLongValue();
        mJsonLength+=String.valueOf(number).length();
        return number;
    }

    public long nextLongValue(long defaultValue) throws IOException {
        long number = mParser.nextLongValue(defaultValue);
        mJsonLength+=String.valueOf(number).length();
        return number;
    }

    public double getDoubleValue() throws IOException {
        double number = mParser.getDoubleValue();
        mJsonLength += String.valueOf(number).length();
        return number;
    }



    public float getFloatValue() throws IOException{
        float number = mParser.getFloatValue();
        mJsonLength+=String.valueOf(number).length();
        return number;
    }

    public long getLength(){
        return mJsonLength;
    }
}
