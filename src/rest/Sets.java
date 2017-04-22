package rest;

import data.queryBuilder.SetsQueryCreator;
import rest.responses.DownloadSetResponse;
import rest.responses.SetsResponse;

import javax.naming.NamingException;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.BrokenBarrierException;
import java.util.logging.Logger;

@Path("/sets")
public class Sets {

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getSets(@QueryParam("name") String name,
                            @QueryParam("l1") long l1,
                            @QueryParam("l2") long l2,
                            @QueryParam("sort") int sorting,
                            @QueryParam("page") int page,
                            @QueryParam("limit") int limit) {
        try {
            Logger logger = Logger.getLogger(getClass().getName());
            logger.severe("Tutaj jeszcze działa");
            //String query = buildQuery(name, l1, l2, sorting, page, limit);
            String query = SetsQueryCreator.getQuery(name, l1, l2, sorting, page, limit);
            /*BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("/select_sets.sql")));
            StringBuffer stringBuffer = new StringBuffer();
            String line = null;
            while((line = bufferedReader.readLine()) != null){
                stringBuffer.append(line).append(" ");
            }
            String query = stringBuffer.toString() + " LIMIT "+ limit;*/
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

    @GET
    @Path("/{setId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSet(@PathParam("setId") long set){
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("set " + set );

        try {
            String response = DownloadSetResponse.create(set);

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

}
