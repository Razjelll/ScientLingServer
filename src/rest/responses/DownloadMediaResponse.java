package rest.responses;

import data.files.MediaFileSystem;

import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class DownloadMediaResponse {

    public enum MediaType{
        IMAGES,
        RECORDS
    }

    public static StreamingOutput create(MediaType mediaType,String catalog){
        StreamingOutput streamingOutput = outputStream -> {

            ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream);
            final File folder = getFileFolder(mediaType, catalog);
            if(!folder.exists()){
                zipOutputStream.close();
                return;
            }
            for(File fileEntry : folder.listFiles()){
                //jesteśmy w folderze obrazków danego zestawu
                ZipEntry entry = new ZipEntry(fileEntry.getName());
                zipOutputStream.putNextEntry(entry);
                FileInputStream inputStream = new FileInputStream(fileEntry);
                byte[] buffer = new byte[1024];
                /*while(inputStream.available()>0){
                    zipOutputStream.write(inputStream.read());
                }*/
                while(inputStream.read(buffer) != -1){
                    zipOutputStream.write(buffer);
                    zipOutputStream.flush();
                }
                zipOutputStream.closeEntry();
                inputStream.close();
            }
            zipOutputStream.close();
        };
        return streamingOutput;
    }

    public static long getLength(MediaType mediaType, String catalog) throws IOException {
        final File foleder = getFileFolder(mediaType, catalog);
        long contentLength = 0;
        for(File fileEntry:foleder.listFiles()){
            contentLength  += fileEntry.length();
        }
        return contentLength;
    }

    private static File getFileFolder(MediaType mediaType, String catalog) throws IOException {
        switch (mediaType){
            case IMAGES:
                return new File(MediaFileSystem.getImageCatalog(catalog));
            case RECORDS:
                return new File(MediaFileSystem.getRecordsCatalog(catalog));
        }
        return null;
    }

}
