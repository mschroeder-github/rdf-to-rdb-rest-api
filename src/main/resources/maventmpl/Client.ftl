package ${package};

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import java.util.StringJoiner;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Client to the server.
 */
public class Client {

    private String host;

    public Client(String host) {
        this.host = host;
    }

    <#list javaClasses as javaClass>
    //==========================================================================
    // ${javaClass.getName()}

    public List<${javaClass.getName()}> get${javaClass.getName()}(int... ids) {
        return get${javaClass.getName()}WithQuery(null, ids);
    }

    public List<${javaClass.getName()}> get${javaClass.getName()}WithQuery(String queryString, int... ids) {
        queryString = (queryString == null || queryString.isEmpty()) ? "" : "?" + queryString;
        JSONObject result = getObject("/${javaClass.getEndpointName()}" + toCommaList(ids) + queryString);
        JSONArray list = result.getJSONArray("list");
        List<${javaClass.getName()}> recordList = new ArrayList<>();
        for(int i = 0; i < list.length(); i++) {
            recordList.add(to${javaClass.getName()}(list.getJSONObject(i)));
        }
        return recordList;
    }

    public String post${javaClass.getName()}(${javaClass.getName()} record) {
        return postAndGetLocation("/${javaClass.getEndpointName()}", toJSONObject(record));
    }

    public void put${javaClass.getName()}(${javaClass.getName()} record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/${javaClass.getEndpointName()}" + "/" + record.getId(), "PUT", toJSONObject(record));
    }

    public void patch${javaClass.getName()}(${javaClass.getName()} record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/${javaClass.getEndpointName()}" + "/" + record.getId(), "PATCH", toJSONObject(record));
    }

    public void delete${javaClass.getName()}(int... ids) {
        send("/${javaClass.getEndpointName()}" + toCommaList(ids), "DELETE", null);
    }

    private JSONObject toJSONObject(${javaClass.getName()} record) {
        JSONObject result = new JSONObject();
    
        <#list javaClass.getSingleAttributes() as attr>
        if(record.has${attr.getMethodName()}()) {
            result.put("${attr.getName()}", record.get${attr.getMethodName()}());
        }
        </#list>

        <#list javaClass.getListAttributes() as attr>
        if(record.has${attr.getMethodName()}() && !record.get${attr.getMethodName()}().isEmpty()) {
            result.put("${attr.getName()}", new JSONArray(record.get${attr.getMethodName()}())); <#-- TODO LangString -->
        }
        </#list>

        return result;
    }

    private ${javaClass.getName()} to${javaClass.getName()}(JSONObject jsonObject) {
        ${javaClass.getName()} record = new ${javaClass.getName()}();

        record.setId(jsonObject.optLong("id"));

        <#list javaClass.getSingleAttributesWithoutId() as attr>
        record.set${attr.getMethodName()}(to(jsonObject.opt("${attr.getName()}"), ${attr.getType()}.class));
        </#list>

        <#if (javaClass.getListAttributes()?size > 0)>
        JSONArray array;
        <#list javaClass.getListAttributes() as attr>
        array = jsonObject.optJSONArray("${attr.getName()}");
        if(array != null && !(array.length() == 0)) {
            ${attr.getType()} list = new ArrayList<>();
            for(int i = 0; i < array.length(); i++) {
                list.add(to(array.get(i), ${attr.getTypeSingle()}.class));
            }
            record.set${attr.getMethodName()}(list);
        }
        </#list>
        </#if>

        return record;
    }
    

    </#list>

    private <T> T to(Object obj, Class<T> type) {
        if(obj == null)
            return (T) obj;

        if(obj instanceof Integer && type.equals(Long.class)) {
            Integer i = (Integer) obj;
            return (T) Long.valueOf(i.longValue());
        }
        return (T) obj;
    }

    private String get(String path) {
        try {
            URL url = new URL(host + path);
            
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setDoOutput(true);
            con.setInstanceFollowRedirects(true);
            con.setRequestMethod("GET");
            con.setRequestProperty("Accept", "application/json");
            con.connect();
            int status = con.getResponseCode();
            
            if(!String.valueOf(status).startsWith("2")) {
                throw new RuntimeException("Response has status " + status);
            }
            
            return IOUtils.toString(con.getInputStream(), "UTF-8");
            
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private JSONObject getObject(String path) {
        return new JSONObject(get(path));
    }

    private String toCommaList(int[] ids) {
        if(ids.length == 0)
            return "";
        
        StringJoiner sj = new StringJoiner(",");
        for(int i = 0; i < ids.length; i++) {
            sj.add(String.valueOf(ids[i]));
        }
        return "/" + sj.toString();
    }

    private String postAndGetLocation(String path, Object json) {
        try {
            URL url = new URL(host + path);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestMethod("POST");
            con.connect();
            
            IOUtils.write(json.toString(), con.getOutputStream(), "UTF-8");
            
            int status = con.getResponseCode();

            if(!String.valueOf(status).startsWith("2")) {
                throw new RuntimeException("Response has status " + status);
            }
            
            String location = con.getHeaderField("Location");
            //con.disconnect();
            
            return location;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void send(String path, String method, Object json) {
        try {
            URL url = new URL(host + path);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            if(json != null) {
                con.setRequestProperty("Content-Type", "application/json");
            }
            con.setRequestMethod(method);
            con.connect();
            
            if(json != null) {
                IOUtils.write(json.toString(), con.getOutputStream(), "UTF-8");
            }

            int status = con.getResponseCode();
            
            if(!String.valueOf(status).startsWith("2")) {
                throw new RuntimeException("Response has status " + status);
            }

        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    //see https://stackoverflow.com/questions/25163131/httpurlconnection-invalid-http-method-patch/46323891#46323891

    private static void allowMethods(String... methods) {
        try {
            Field methodsField = HttpURLConnection.class.getDeclaredField("methods");

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(methodsField, methodsField.getModifiers() & ~Modifier.FINAL);

            methodsField.setAccessible(true);

            String[] oldMethods = (String[]) methodsField.get(null);
            Set<String> methodsSet = new LinkedHashSet<>(Arrays.asList(oldMethods));
            methodsSet.addAll(Arrays.asList(methods));
            String[] newMethods = methodsSet.toArray(new String[0]);

            methodsField.set(null/*static field*/, newMethods);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    static {
        allowMethods("PATCH");
    }

}
