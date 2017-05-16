package rest.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;


public class SetImagesRequest {

    private long mId;

    public SetImagesRequest(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(json);
        mId = node.path("id").asLong();
    }

    public long getId(){
        return mId;
    }

}
