package data.files;

import java.io.IOException;
import java.io.InputStream;

public class Records {

    public static void insert(String catalog, InputStream inputStream) throws IOException {
        new FileInsert().insertFromZip(MediaFileSystem.getRecordsCatalog(catalog), inputStream);
    }
}
