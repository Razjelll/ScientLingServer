package data.Properties;

import java.io.IOException;
import java.util.Properties;

public class FileProps {

    private static final String IMAGES_PATH = "images_path";
    private static final String RECORDS_PATH = "records_path";

    public static String getImagesPath() throws IOException {
        Properties properties = new PropertiesLoader().loadProperties();
        return properties.getProperty(IMAGES_PATH);
    }

    public static String getRecordsPath() throws IOException{
        Properties properties = new PropertiesLoader().loadProperties();
        return properties.getProperty(RECORDS_PATH);
    }

}
