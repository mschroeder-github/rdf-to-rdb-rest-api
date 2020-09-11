package rdf2rdb;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

/**
 *
 */
public class REST {
    
    public static boolean DEBUG = true;
    
    public static String get(String urlspec) {
        try {
            if(DEBUG)
                System.out.println("get " + urlspec);
            
            URL url = new URL(urlspec);
            
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setDoOutput(true);
            con.setInstanceFollowRedirects(true);
            con.setRequestMethod("GET");
            con.setRequestProperty("Accept", "application/json");
            con.connect();
            int status = con.getResponseCode();
            
            return IOUtils.toString(con.getInputStream(), "UTF-8");
            
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static JSONObject getObject(String urlspec) {
        JSONObject resp = new JSONObject(get(urlspec));
        //System.out.println("answer of " + urlspec + ":");
        //System.out.println(resp.toString(2));
        return resp;
    }
    
}
