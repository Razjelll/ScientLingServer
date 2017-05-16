package data;

import java.io.*;
import java.util.Scanner;

/**
 * Created by Razjelll on 12.04.2017.
 */
public class FileReader {

    public static String readFile(String filePath) throws IOException {
        /*FileInsert file = new FileInsert(filePath);
        if(file== null){
            return null;
        }
        Scanner scanner = new Scanner(file);
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        while(scanner.hasNext()){
            line = scanner.nextLine();
            stringBuilder.append(line).append(" ");
        }
        return stringBuilder.toString();*/

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath)));
        StringBuffer stringBuffer = new StringBuffer();
        String line = null;
        while((line = bufferedReader.readLine()) != null){
            stringBuffer.append(line).append(" ");
        }
        return stringBuffer.toString();
    }
}
