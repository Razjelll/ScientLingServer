package rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import rest.responses.RegistrationResponse;

import javax.naming.NamingException;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Created by Razjelll on 20.04.2017.
 */
@Path("/registration")
public class Registration {

    private final  String LOGIN = "login";
    private final String EMAIL = "email";
    private final String PASSWORD = "password";

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response register(final String request){
        Logger logger = Logger.getLogger(getClass().getName());
        logger.severe("register");
        ObjectMapper mapper = new ObjectMapper();
        try{
            JsonNode node = mapper.readTree(request);
            String login = node.get(LOGIN).asText();
            String email = node.get(EMAIL).asText();
            String password = node.get(PASSWORD).asText();

            Response response = RegistrationResponse.create(login, email, password);
            return response;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }
}
