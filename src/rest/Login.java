package rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.JSONPObject;
import data.DatabaseConnector;
import jdk.nashorn.internal.parser.JSONParser;
import rest.responses.LoginResponse;

import javax.naming.NamingException;
import javax.transaction.Status;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Created by Razjelll on 19.04.2017.
 */
@Path("/login")
public class Login {

    private final String USERNAME = "username";
    private final String PASSWORD = "password";

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response login(final String request)
    {
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("post " + request);

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(request);
            String username = node.get(USERNAME).asText();
            String password = node.get(PASSWORD).asText();
            logger.severe(username + password);

            String response = LoginResponse.create(username, password);
            logger.severe(response);
            if(response!= null && !response.equals("{}")){
                return Response.ok().entity(response).build();
            } else {
                return Response.status(401).build();
            }
        } catch (IOException e) {
            return Response.status(Status.STATUS_UNKNOWN).build();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return Response.ok().build();
    }
}
