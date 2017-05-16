package data.utils;

/**
 * Created by Razjelll on 05.05.2017.
 */
public class StringLengthCalculator {

    //w UTF-8 liczba bitów jest równa długości łańcuchu
    private static final int MULTIPLIER = 1;

    public static int getBytesCount(int length){
        return length * MULTIPLIER;
    }

    public static float getSize(int length){
        return length * MULTIPLIER/1024;
    }
}
