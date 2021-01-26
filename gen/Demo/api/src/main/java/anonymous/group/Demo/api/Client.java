package anonymous.group.Demo.api;

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

    //==========================================================================
    // Book

    public List<Book> getBook(int... ids) {
        return getBookWithQuery(null, ids);
    }

    public List<Book> getBookWithQuery(String queryString, int... ids) {
        queryString = (queryString == null || queryString.isEmpty()) ? "" : "?" + queryString;
        JSONObject result = getObject("/book" + toCommaList(ids) + queryString);
        JSONArray list = result.getJSONArray("list");
        List<Book> recordList = new ArrayList<>();
        for(int i = 0; i < list.length(); i++) {
            recordList.add(toBook(list.getJSONObject(i)));
        }
        return recordList;
    }

    public String postBook(Book record) {
        return postAndGetLocation("/book", toJSONObject(record));
    }

    public void putBook(Book record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/book" + "/" + record.getId(), "PUT", toJSONObject(record));
    }

    public void patchBook(Book record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/book" + "/" + record.getId(), "PATCH", toJSONObject(record));
    }

    public void deleteBook(int... ids) {
        send("/book" + toCommaList(ids), "DELETE", null);
    }

    private JSONObject toJSONObject(Book record) {
        JSONObject result = new JSONObject();
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasHasCover()) {
            result.put("hasCover", record.getHasCover());
        }
        if(record.hasPublishedDate()) {
            result.put("publishedDate", record.getPublishedDate());
        }
        if(record.hasTakesPlaceIn()) {
            result.put("takesPlaceIn", record.getTakesPlaceIn());
        }

        if(record.hasName() && !record.getName().isEmpty()) {
            result.put("name", new JSONArray(record.getName())); 
        }

        return result;
    }

    private Book toBook(JSONObject jsonObject) {
        Book record = new Book();

        record.setId(jsonObject.optLong("id"));

        record.setHasCover(to(jsonObject.opt("hasCover"), String.class));
        record.setPublishedDate(to(jsonObject.opt("publishedDate"), Long.class));
        record.setTakesPlaceIn(to(jsonObject.opt("takesPlaceIn"), Long.class));

        JSONArray array;
        array = jsonObject.optJSONArray("name");
        if(array != null && !(array.length() == 0)) {
            List<LangString> list = new ArrayList<>();
            for(int i = 0; i < array.length(); i++) {
                list.add(to(array.get(i), LangString.class));
            }
            record.setName(list);
        }

        return record;
    }
    

    //==========================================================================
    // CapitalCity

    public List<CapitalCity> getCapitalCity(int... ids) {
        return getCapitalCityWithQuery(null, ids);
    }

    public List<CapitalCity> getCapitalCityWithQuery(String queryString, int... ids) {
        queryString = (queryString == null || queryString.isEmpty()) ? "" : "?" + queryString;
        JSONObject result = getObject("/capitalCity" + toCommaList(ids) + queryString);
        JSONArray list = result.getJSONArray("list");
        List<CapitalCity> recordList = new ArrayList<>();
        for(int i = 0; i < list.length(); i++) {
            recordList.add(toCapitalCity(list.getJSONObject(i)));
        }
        return recordList;
    }

    public String postCapitalCity(CapitalCity record) {
        return postAndGetLocation("/capitalCity", toJSONObject(record));
    }

    public void putCapitalCity(CapitalCity record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/capitalCity" + "/" + record.getId(), "PUT", toJSONObject(record));
    }

    public void patchCapitalCity(CapitalCity record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/capitalCity" + "/" + record.getId(), "PATCH", toJSONObject(record));
    }

    public void deleteCapitalCity(int... ids) {
        send("/capitalCity" + toCommaList(ids), "DELETE", null);
    }

    private JSONObject toJSONObject(CapitalCity record) {
        JSONObject result = new JSONObject();
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }


        return result;
    }

    private CapitalCity toCapitalCity(JSONObject jsonObject) {
        CapitalCity record = new CapitalCity();

        record.setId(jsonObject.optLong("id"));



        return record;
    }
    

    //==========================================================================
    // Country

    public List<Country> getCountry(int... ids) {
        return getCountryWithQuery(null, ids);
    }

    public List<Country> getCountryWithQuery(String queryString, int... ids) {
        queryString = (queryString == null || queryString.isEmpty()) ? "" : "?" + queryString;
        JSONObject result = getObject("/country" + toCommaList(ids) + queryString);
        JSONArray list = result.getJSONArray("list");
        List<Country> recordList = new ArrayList<>();
        for(int i = 0; i < list.length(); i++) {
            recordList.add(toCountry(list.getJSONObject(i)));
        }
        return recordList;
    }

    public String postCountry(Country record) {
        return postAndGetLocation("/country", toJSONObject(record));
    }

    public void putCountry(Country record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/country" + "/" + record.getId(), "PUT", toJSONObject(record));
    }

    public void patchCountry(Country record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/country" + "/" + record.getId(), "PATCH", toJSONObject(record));
    }

    public void deleteCountry(int... ids) {
        send("/country" + toCommaList(ids), "DELETE", null);
    }

    private JSONObject toJSONObject(Country record) {
        JSONObject result = new JSONObject();
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasHasCapitalCity()) {
            result.put("hasCapitalCity", record.getHasCapitalCity());
        }


        return result;
    }

    private Country toCountry(JSONObject jsonObject) {
        Country record = new Country();

        record.setId(jsonObject.optLong("id"));

        record.setHasCapitalCity(to(jsonObject.opt("hasCapitalCity"), Long.class));


        return record;
    }
    

    //==========================================================================
    // Page

    public List<Page> getPage(int... ids) {
        return getPageWithQuery(null, ids);
    }

    public List<Page> getPageWithQuery(String queryString, int... ids) {
        queryString = (queryString == null || queryString.isEmpty()) ? "" : "?" + queryString;
        JSONObject result = getObject("/page" + toCommaList(ids) + queryString);
        JSONArray list = result.getJSONArray("list");
        List<Page> recordList = new ArrayList<>();
        for(int i = 0; i < list.length(); i++) {
            recordList.add(toPage(list.getJSONObject(i)));
        }
        return recordList;
    }

    public String postPage(Page record) {
        return postAndGetLocation("/page", toJSONObject(record));
    }

    public void putPage(Page record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/page" + "/" + record.getId(), "PUT", toJSONObject(record));
    }

    public void patchPage(Page record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/page" + "/" + record.getId(), "PATCH", toJSONObject(record));
    }

    public void deletePage(int... ids) {
        send("/page" + toCommaList(ids), "DELETE", null);
    }

    private JSONObject toJSONObject(Page record) {
        JSONObject result = new JSONObject();
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasBookHasPage()) {
            result.put("bookHasPage", record.getBookHasPage());
        }
        if(record.hasPaperbackHasPage()) {
            result.put("paperbackHasPage", record.getPaperbackHasPage());
        }

        if(record.hasMentionsCapitalCity() && !record.getMentionsCapitalCity().isEmpty()) {
            result.put("mentionsCapitalCity", new JSONArray(record.getMentionsCapitalCity())); 
        }

        return result;
    }

    private Page toPage(JSONObject jsonObject) {
        Page record = new Page();

        record.setId(jsonObject.optLong("id"));

        record.setBookHasPage(to(jsonObject.opt("bookHasPage"), Long.class));
        record.setPaperbackHasPage(to(jsonObject.opt("paperbackHasPage"), Long.class));

        JSONArray array;
        array = jsonObject.optJSONArray("mentionsCapitalCity");
        if(array != null && !(array.length() == 0)) {
            List<Long> list = new ArrayList<>();
            for(int i = 0; i < array.length(); i++) {
                list.add(to(array.get(i), Long.class));
            }
            record.setMentionsCapitalCity(list);
        }

        return record;
    }
    

    //==========================================================================
    // Paperback

    public List<Paperback> getPaperback(int... ids) {
        return getPaperbackWithQuery(null, ids);
    }

    public List<Paperback> getPaperbackWithQuery(String queryString, int... ids) {
        queryString = (queryString == null || queryString.isEmpty()) ? "" : "?" + queryString;
        JSONObject result = getObject("/paperback" + toCommaList(ids) + queryString);
        JSONArray list = result.getJSONArray("list");
        List<Paperback> recordList = new ArrayList<>();
        for(int i = 0; i < list.length(); i++) {
            recordList.add(toPaperback(list.getJSONObject(i)));
        }
        return recordList;
    }

    public String postPaperback(Paperback record) {
        return postAndGetLocation("/paperback", toJSONObject(record));
    }

    public void putPaperback(Paperback record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/paperback" + "/" + record.getId(), "PUT", toJSONObject(record));
    }

    public void patchPaperback(Paperback record) {
        if(!record.hasId()) {
            throw new IllegalArgumentException("The record needs an ID. Use setId method.");
        }
        send("/paperback" + "/" + record.getId(), "PATCH", toJSONObject(record));
    }

    public void deletePaperback(int... ids) {
        send("/paperback" + toCommaList(ids), "DELETE", null);
    }

    private JSONObject toJSONObject(Paperback record) {
        JSONObject result = new JSONObject();
    
        if(record.hasId()) {
            result.put("id", record.getId());
        }
        if(record.hasHasCover()) {
            result.put("hasCover", record.getHasCover());
        }
        if(record.hasTakesPlaceIn()) {
            result.put("takesPlaceIn", record.getTakesPlaceIn());
        }


        return result;
    }

    private Paperback toPaperback(JSONObject jsonObject) {
        Paperback record = new Paperback();

        record.setId(jsonObject.optLong("id"));

        record.setHasCover(to(jsonObject.opt("hasCover"), String.class));
        record.setTakesPlaceIn(to(jsonObject.opt("takesPlaceIn"), Long.class));


        return record;
    }
    


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
