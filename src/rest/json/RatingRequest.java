package rest.json;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class RatingRequest {
    private int mRating;

    public int getRating(){return mRating;}

    private final String RATING = "rating";

    public RatingRequest(String json) throws IOException {
        /*JSONParser parser = new JSONParser();
        JSONObject object = (JSONObject)parser.parse(json);
        mRating = (int)object.get(RATING);*/
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(json);
        mRating = node.path(RATING).asInt();
    }
}
