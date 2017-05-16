package rest.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.json.Json;
import java.io.IOException;

public class DescriptionRequest {

    private static String DESCRIPTION = "desc";

    public static String getDescription(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(json);
        String description = node.get(DESCRIPTION).asText();
        return description;
    }
}
