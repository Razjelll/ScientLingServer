package data.files;

import java.io.*;

public class Images {

    private static final int BUFFER_SIZE = 1024;

    public static void insert(String catalog, InputStream inputStream) throws IOException {

        new FileInsert().insertFromZip(MediaFileSystem.getImageCatalog(catalog), inputStream);
    }

}
