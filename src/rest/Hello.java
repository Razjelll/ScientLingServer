package rest;

import data.Properties.FileProps;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

@Path("/hello")
public class Hello {

    @GET
    public String sayPlainTextHello() throws IOException {
        String folderPath = FileProps.getImagesPath();
        File file = new File(FileProps.getImagesPath()+"/plik.text");
        file.createNewFile();
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        OutputStreamWriter writer =  new OutputStreamWriter(fileOutputStream);
        writer.write("udało sie");
        writer.close();

        return "Hello Jersey";
    }

}
