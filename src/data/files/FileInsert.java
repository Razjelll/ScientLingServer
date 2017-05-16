package data.files;

import data.Properties.FileProps;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileInsert {

    private final int BUFFER_SIZE = 1024;

    public void insertFromZip(String catalog, InputStream inputStream) throws IOException {
        ZipInputStream zipInputStream = new ZipInputStream(inputStream);
        File imageCatalog= new File(catalog);
        if(!imageCatalog.exists()){
            imageCatalog.mkdir();
        }
        ZipEntry zipEntry;
        byte[] buffer = new byte[BUFFER_SIZE];
        while((zipEntry = zipInputStream.getNextEntry()) != null){
            File imageFile = new File(catalog+"/"+zipEntry.getName());
            imageCatalog.createNewFile();
            FileOutputStream outputStream = new FileOutputStream(imageFile);
            while(zipInputStream.available()>0){
                outputStream.write(zipInputStream.read());
            }
            outputStream.close();
        }
        zipInputStream.close();
    }


}
