package rest;

import data.DatabaseConnector;
import data.SetParser;
import data.files.*;
import org.glassfish.jersey.media.multipart.BodyPart;
import rest.json.RatingRequest;
import rest.json.SetImagesRequest;
import data.queryExecutors.*;
import data.utils.SetOwnership;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import rest.authentication.AuthenticationReader;
import rest.json.DescriptionRequest;
import rest.responses.*;

import javax.naming.NamingException;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.*;
import java.sql.*;
import java.util.logging.Logger;

@Path("/sets")
public class Sets {

    @GET
    public Response getSets(@QueryParam("name") String name,
                            @QueryParam("l1") long l1,
                            @QueryParam("l2") long l2,
                            @QueryParam("sort") int sorting,
                            @QueryParam("page") int page,
                            @QueryParam("limit") int limit) {
        try {
            Logger logger = Logger.getLogger(getClass().getName());
            logger.severe("Tutaj jeszcze działa");
            String response = SetsResponse.create(name, l1, l2, sorting, page, limit);
            return Response.status(Response.Status.OK).entity(response).build();
        } catch (IOException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } catch (SQLException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        } catch (NamingException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } catch (ClassNotFoundException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @POST
    public Response uploadSet(final InputStream stream, @HeaderParam("Authorization") String auth
                              /*@Context HttpServletRequest request*/) throws IOException, SQLException, NamingException, ClassNotFoundException {
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("WEszło");

        String username = new AuthenticationReader(auth).getUsername();
        long userId = SelectUserIdExecutor.getUserId(username);

        //int contentLength = request.getContentLength();
        //new Thread(new SaveSetRunnable(stream)).start();
        SetParser parser = new SetParser(stream);
        parser.setUderId(userId);
        //SetParser.ProcessResult result = parser.process();
        long setId = parser.process();
        if(setId>0){
            String responseJson = UploadSetResponse.create(setId);
            return Response.ok().entity(responseJson).build();
        }
        //TODO ustawić odpowiedni status
        return Response.status(500).build();

    }

    @GET
    @Path("/{setId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSet(@HeaderParam("Authorization") String auth,
                           @PathParam("setId") long setId){
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("set " + setId );

        try {
            String response = DownloadSetResponse.create(setId);
            Connection connection = DatabaseConnector.getConnection();
            String username = new AuthenticationReader(auth).getUsername();
            long userId = SelectUserIdExecutor.getUserId(username);
            DownloadsExecutor.insert(userId, setId, connection);
            /*SetExecutor executor = new SetExecutor(connection);
            executor.increaseDownloadCount(set);*/
            connection.close();
            return Response.ok().entity(response).build();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return Response.ok().build();
    }

    @GET
    @Path("/{setId}/details")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSetDetails(@HeaderParam("Authorization") String auth,
                                  @PathParam("setId") long setId
                                  /*String json*/){
        try {
            String username = new AuthenticationReader(auth).getUsername();
            long userId = SelectUserIdExecutor.getUserId(username);
            /*ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(json);
            String username = node.path("username").asText();
            long userId = SelectUserIdExecutor.getUserId(username);*/
            String response = SetDetailsResponse.create(setId, userId);
            if(response != null){
                return Response.ok().entity(response).build();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Response.noContent().build();
    }



    @POST
    @Path("/images")
    @Consumes({MediaType.MULTIPART_FORM_DATA})
    public Response setImages(@HeaderParam("Authorization") String auth,
                              @FormDataParam("json") String json,
                              @FormDataParam("data") InputStream inputStream,
                              @FormDataParam("data")FormDataContentDisposition fileDetail
                              ) throws IOException, SQLException, NamingException, ClassNotFoundException {
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("Upload images");
        long id = new SetImagesRequest(json).getId();
        Connection connection = DatabaseConnector.getConnection();
        //sprawdzamy czy dany użytkownik wstaił ten zestaw
        String username = new AuthenticationReader(auth).getUsername();
        if(!new CheckAuthorExecutor(connection).check(id, username)){
            //jeżeli nie ma uprawnień do wstawienia zdjęć przerywamy i zwracamy kod do użytkownika
            return Response.status(401).build();
        }
        //pobieramy nazwę katalogu w którym mają zostać zapisane obrazki
        String catalog = new SelectSetExecutor(connection).getCatalog(id);
        connection.close();
        //wstawiamy obrazki ze strumienia
        Images.insert(catalog,inputStream);
        return Response.ok().build();
    }

    @POST
    @Path("/records")
    @Consumes({MediaType.MULTIPART_FORM_DATA})
    public Response setRecords(@HeaderParam("Authorization") String auth,
                               @FormDataParam("json") String json,
                               @FormDataParam("data") InputStream inputStream,
                               @FormDataParam("data")FormDataContentDisposition fileDetail) throws IOException, SQLException, NamingException, ClassNotFoundException {
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("Upload records");
        long id = new SetImagesRequest(json).getId();
        Connection connection = DatabaseConnector.getConnection();
        //sprawdzamy czy dany użytkownik wstaił ten zestaw
        String username = new AuthenticationReader(auth).getUsername();
        if(!new CheckAuthorExecutor(connection).check(id, username)){
            //jeżeli nie ma uprawnień do wstawienia zdjęć przerywamy i zwracamy kod do użytkownika
            return Response.status(401).build();
        }
        //pobieramy nazwę katalogu w którym mają zostać zapisane obrazki
        String catalog = new SelectSetExecutor(connection).getCatalog(id);
        connection.close();
        //wstawiamy obrazki ze strumienia
        Records.insert(catalog,inputStream);
        return Response.ok().build();
    }

    @POST
    @Path("/{setId}/images")
    public Response setImages(@HeaderParam("Authorization") String auth,
                               @PathParam("setId") long setId,
                               InputStream inputStream) throws IOException, SQLException, NamingException, ClassNotFoundException {
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("Noqwe wysyłanie obrazków");
        Connection connection = DatabaseConnector.getConnection();
        String username = new AuthenticationReader(auth).getUsername();
        if(!new CheckAuthorExecutor(connection).check(setId, username)){
            //jeżeli nie ma uprawnień do wstawienia zdjęć przerywamy i zwracamy kod do użytkownika
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        String catalog = new SelectSetExecutor(connection).getCatalog(setId);
        connection.close();
        Images.insert(catalog, inputStream);
        return Response.ok().build();
    }

    @POST
    @Path("/{setId}/records")
    public Response setRecords(@HeaderParam("Authorization") String auth,
                               @PathParam("setId") long setId,
                               InputStream inputStream) throws SQLException, NamingException, ClassNotFoundException, IOException {
        Connection connection = DatabaseConnector.getConnection();
        String username = new AuthenticationReader(auth).getUsername();
        if(!new CheckAuthorExecutor(connection).check(setId, username)){
            //jeżeli nie ma uprawnień do wstawienia zdjęć przerywamy i zwracamy kod do użytkownika
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        String catalog = new SelectSetExecutor(connection).getCatalog(setId);
        connection.close();
        Records.insert(catalog, inputStream);
        return Response.ok().build();
    }

    @GET
    @Path("/{setId}/images")
    public Response getImages(@PathParam("setId") long set)
            throws SQLException, NamingException, ClassNotFoundException, IOException {
        Connection connection = DatabaseConnector.getConnection();
        String catalog = new SelectSetExecutor(connection).getCatalog(set);
        long contentLength = DownloadMediaResponse.getLength(DownloadMediaResponse.MediaType.IMAGES, catalog);
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("Content length: " + contentLength);
        StreamingOutput streamingOutput = DownloadMediaResponse.create(DownloadMediaResponse.MediaType.IMAGES,catalog);
        return Response.ok().header("Content-Length", contentLength).entity(streamingOutput).build();
        //return Response.ok().header("X-Content-Length", contentLength).entity(streamingOutput).build();
        //return Response.ok().entity(streamingOutput).build();
    }

    @GET
    @Path("/{setId}/records")
    public Response getRecords(@PathParam("setId") long set) throws IOException, SQLException, NamingException, ClassNotFoundException {
        Connection connection = DatabaseConnector.getConnection();
        String catalog = new SelectSetExecutor(connection).getCatalog(set);
        StreamingOutput streamingOutput = DownloadMediaResponse.create(DownloadMediaResponse.MediaType.RECORDS,catalog);
        return Response.ok().entity(streamingOutput).build();
    }

    @GET
    @Path("/users/{user}")
    public Response getUsersSets(@PathParam("user") String user) throws SQLException, NamingException, ClassNotFoundException, IOException {
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("Znalazło");
        String setsJson = UsersSetsResponse.create(user);
        if(setsJson != null){
            return Response.ok().entity(setsJson).build();
        }
        return Response.status(500).build();
    }

    @DELETE
    @Path("/{setId}/images")
    public Response deleteImages(@HeaderParam("Authorization") String auth,
                                 @PathParam("setId") long setId) throws SQLException, NamingException, ClassNotFoundException, IOException {
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("start ");
        if(!SetOwnership.check(setId, auth)){
            return Response.status(401).build();
        }
        logger.severe(String.valueOf(setId));
        Connection connection = DatabaseConnector.getConnection();
        String catalog = new SelectSetExecutor(connection).getCatalog(setId);
        connection.close();
        MediaFileSystem.deleteImageCatalog(catalog);
        return Response.ok().build();
    }

    @DELETE
    @Path("/{setId}/records")
    public Response deleteRecords(@HeaderParam("Authorization") String auth,
                                  @PathParam("setId") long setId) throws SQLException, NamingException, ClassNotFoundException, IOException {
        if(!SetOwnership.check(setId, auth)){
            return Response.status(401).build();
        }
        Connection connection = DatabaseConnector.getConnection();
        String catalog = new SelectSetExecutor(connection).getCatalog(setId);
        connection.close();
        MediaFileSystem.deleteRecordsCatalog(catalog);
        return Response.ok().build();
    }

    @DELETE
    @Path("/{setId}")
    public Response deleteSet(@HeaderParam("Authorization") String auth,
                              @PathParam("setId") long setId) throws SQLException, NamingException, ClassNotFoundException, IOException {
        if(!SetOwnership.check(setId, auth)){
            return Response.status(401).build();
        }
        Connection connection = DatabaseConnector.getConnection();

        String catalog = new SelectSetExecutor(connection).getCatalog(setId);
        MediaFileSystem.deleteRecordsCatalog(catalog);
        MediaFileSystem.deleteImageCatalog(catalog);
        DeleteExecutor.deleteSet(setId, connection);
        connection.close();
        return Response.ok().build();
    }

    @PUT
    @Path("/{setId}/description")
    public Response changeDescription(@HeaderParam("Authorization") String auth,
                                      @PathParam("setId") long setId,
                                      String json) throws IOException, SQLException, NamingException, ClassNotFoundException {
        if(!SetOwnership.check(setId, auth)){
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        Connection connection = DatabaseConnector.getConnection();
        SetExecutor executor = new SetExecutor(connection);
        String description = DescriptionRequest.getDescription(json);
        executor.updateDescription(setId, description);
        connection.close();

        return Response.ok().build();
    }

    @GET
    @Path("/{setId}/description")
    public Response getDesription(@PathParam("setId") long setId) throws SQLException, NamingException, ClassNotFoundException, IOException {
        String result = DescriptionResponse.create(setId);
        if(result != null){
            return Response.ok().entity(result).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    @PUT
    @Path("/{setId}/rating")
    public Response getDescription(@HeaderParam("Authorization") String auth,
                                   @PathParam("setId") long setId,
                                   String json) throws IOException, ClassNotFoundException, SQLException, NamingException {
        String username = new AuthenticationReader(auth).getUsername();
        long userId = SelectUserIdExecutor.getUserId(username);
        int rating = new RatingRequest(json).getRating();
        Connection connection = DatabaseConnector.getConnection();
        DownloadsExecutor.setRating(userId, setId, rating,connection);
        connection.close();

        return Response.ok().build();
    }

}
