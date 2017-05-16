package rest.authentication;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.StringTokenizer;

/**
 * Created by Razjelll on 04.05.2017.
 */
public class AuthenticationReader {
    private String mUsername;
    private String mPassword;

    public AuthenticationReader(String authCredentials) throws UnsupportedEncodingException {
        if(authCredentials!=null){
            final String encodeUserPassword = authCredentials.replaceFirst("Basic ","");
            String usernameAndPassword = null;
            byte[]decodeBytes = Base64.getDecoder().decode(encodeUserPassword);
            usernameAndPassword = new String(decodeBytes, "UTF-8");
            final StringTokenizer tokenizer = new StringTokenizer(usernameAndPassword, ":");
            mUsername = tokenizer.nextToken();
            mPassword = tokenizer.nextToken();
        }
    }

    public String getUsername(){
        return mUsername;
    }

    public String getPassword(){
        return mPassword;
    }
}
