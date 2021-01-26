package anonymous.group.Demo.server;

import java.util.*;
import org.json.*;
import spark.Request;
import spark.Response;
import spark.Spark;
import anonymous.group.Demo.api.*;
import cz.jirutka.rsql.parser.ast.Node;

public class BookResource {

    private DatabaseController dbc;

    public BookResource(DatabaseController dbc) {
        this.dbc = dbc;

        Spark.get("/book", this::getBook);
        Spark.get("/book/:id", this::getBookId);

        Spark.delete("/book", this::deleteBook);
        Spark.delete("/book/:id", this::deleteBookId);

        Spark.post("/book", this::postBook);

        Spark.put("/book/:id", (req, resp) -> putOrPatchBookId(req, resp, true));
        Spark.patch("/book/:id", (req, resp) -> putOrPatchBookId(req, resp, false));

        Spark.options("/book", this::optionsBook);
    }

    public Object getBook(Request req, Response resp) {
        return getBook(req, resp, null);
    } 

    public Object getBookId(Request req, Response resp) {
        return getBook(req, resp, getIds(req));
    }

    private Object getBook(Request req, Response resp, List<Long> ids) {
        
        String limitStr = req.queryParamOrDefault("limit", null);
        Integer limit = limitStr == null ? null : Integer.parseInt(limitStr);

        String offsetStr = req.queryParamOrDefault("offset", null);
        Integer offset = offsetStr == null ? null : Integer.parseInt(offsetStr);

        String rqlStr = req.queryParamOrDefault("rql", null);
        Node rqlNode = null;
        if(rqlStr != null) {
            rqlNode = SqlUtils.toRQL(rqlStr);
        }

        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);

        result.put("type", "Book");
        
        List<Book> records = dbc.selectBook(ids, offset, limit, rqlNode);
        result.put("list", toJSONArray(records));

        resp.type("application/json");
        return result.toString(2);
    }

    public Object postBook(Request req, Response resp) {
        JSONObject json = toJSONObject(req);
        Book record = fromJSONObject(json);
        dbc.insertBook(Arrays.asList(record));
        resp.header("Location", "/book/" + record.getId());
        resp.status(201);
        return "";
    }

    public Object putOrPatchBookId(Request req, Response resp, boolean put) {
        List<Long> ids = getIds(req);
        JSONObject json = toJSONObject(req);
        Book record = fromJSONObject(json);
        record.setId(ids.get(0));
        List<Book> records = dbc.selectBook(ids, null, null, null);
        if(records.isEmpty()) {
            resp.status(404);
            return "no Book with id " + ids.get(0);
        }
        dbc.updateBook(Arrays.asList(record), put);
        resp.status(204);
        return "";
    }

    public Object deleteBook(Request req, Response resp) {
        dbc.deleteBook();
        resp.status(204);
        return "";
    }

    public Object deleteBookId(Request req, Response resp) {
        dbc.deleteBook(getIds(req));
        resp.status(204);
        return "";
    }

    public Object optionsBook(Request req, Response resp) {
        JSONObject preview = toJSONObjectPreview();
        resp.header("Allow", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
        resp.type("application/json");
        return preview.toString(2);
    }

    private List<Long> getIds(Request req) {
        List<Long> ids = new ArrayList<>();
        for(String idstr : req.params("id").split(",")) {
            try {
                ids.add(Long.parseLong(idstr));
            } catch(Exception e) {
                throw new RuntimeException("id must be a number", e);
            } 
        }
        if(ids.isEmpty()) {
            throw new RuntimeException("no id is given");
        }
        return ids;
    }

    private JSONObject toJSONObject(Request req) {
        try {
            return new JSONObject(req.body());
        } catch(Exception e) {
            throw new RuntimeException("request could not be parsed to JSON", e);
        } 
    }

    public JSONArray toJSONArray(List<Book> records) {
        JSONArray array = new JSONArray();
        for(Book record : records) {
            array.put(toJSONObject(record));
        }
        return array;
    }

    public JSONObject toJSONObject(Book record) {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);
    
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

    public JSONObject toJSONObjectPreview() {
        JSONObject result = new JSONObject();
        JSONUtils.forceLinkedHashMap(result);

        result.put("id", "Long");
        result.put("hasCover", "String");
        result.put("publishedDate", "Long");
        result.put("takesPlaceIn", "Long");

        result.put("name", "JSONArray<LangString>");

        return result;
    }

    public Book fromJSONObject(JSONObject jsonObject) {
        Book record = new Book();

        record.setHasCover(to(jsonObject.opt("hasCover"), String.class));
        record.setPublishedDate(to(jsonObject.opt("publishedDate"), Long.class));
        record.setTakesPlaceIn(to(jsonObject.opt("takesPlaceIn"), Long.class));

        JSONArray array;
        array = jsonObject.optJSONArray("name");
        if(array != null && !array.isEmpty()) {
            List<LangString> list = new ArrayList<>();
            for(int i = 0; i < array.length(); i++) {
                list.add(to(array.get(i), LangString.class));
            }
            record.setName(list);
        }

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

}