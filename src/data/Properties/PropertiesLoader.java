package data.Properties;

import javax.annotation.Resource;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.ws.rs.core.Context;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;
import java.io.*;
import java.net.URL;
import java.util.Properties;
import java.util.ResourceBundle;


public class PropertiesLoader {
    @Resource
    private WebServiceContext mWsContext;


    private ServletContext mContext;

    @Context
    public void setContext(ServletContext context){
        mContext = context;
    }

    public Properties loadProperties() throws IOException{
        Properties properties = new Properties();
        InputStream inputStream = this.getClass().getResourceAsStream("filesystem.properties");
        //String path = ResourceBundle.getBundle("filesystem").getString("images_path");
        if(inputStream != null){
            try {
                properties.load(inputStream);
            } finally {
                inputStream.close();
            }
        }
        return properties;
    }
}
