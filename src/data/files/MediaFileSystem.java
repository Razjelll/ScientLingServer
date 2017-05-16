package data.files;

import data.Properties.FileProps;

import java.io.File;
import java.io.IOException;

public class MediaFileSystem {

    private final static String IMAGES = "images";
    private final static String RECORDS = "records";

    public static String getImageCatalog(String catalog) throws IOException {
        return FileProps.getImagesPath() + "/" + catalog;
    }

    public static String getRecordsCatalog(String catalog) throws IOException {
        return FileProps.getRecordsPath() + "/" + catalog;
    }

    public static boolean deleteImageCatalog(String catalog) throws IOException {
        return deleteMediaCatalog(getImageCatalog(catalog));
    }

    public static boolean deleteRecordsCatalog(String catalog) throws IOException {
        return deleteMediaCatalog(getRecordsCatalog(catalog));
    }

    private static boolean deleteMediaCatalog(String catalogPath){
        File file = new File(catalogPath);
        return deleteDirectory(file);
    }

    public static long getImagesSize(String catalog) throws IOException {
        return getSize(getImageCatalog(catalog));
    }

    public static long getRecordsSize(String catalog) throws IOException {
        return getSize(getRecordsCatalog(catalog));
    }

    private static long getSize(String catalogPath){
        File directory = new File(catalogPath);
        if(!directory.exists()){
            return 0;
        }
        long size = 0;
        for(File fileEntry : directory.listFiles()){
            size += fileEntry.length();
        }
        return size;
    }

    private static boolean deleteDirectory(File directory){
        if(directory.isDirectory()){
            for(File fileEntry : directory.listFiles()){
                fileEntry.delete();
            }
        }
        return directory.delete();
    }
}
